
import random, time, struct, traceback
from threading import Thread

from .. import util
from .. import wallet
from .. import consensus

MINING_AFTER_START = 90      # 90 seconds

_pow_thread  = None  # wait re-assign
_poet_thread = None  # wait re-assign, IO: exit/cancelCurr/appendTask/getResult

class MinePool(object):
  def __init__(self):
    self.no_mining = True
    self.start_time = time.time()
    self.state = 'sync'            # sync, mining
    self.mining_info = ['waiting'] # task can be: 'waiting', 'try_pow', 'get_pow'
    self.main_hi = 0               # mainchain height
    self.incomplete_hi = 0         # incomplete_hi is first incomplete block (no deep check yet)
    self._cancel_reason = ''       # for debuging
  
  def config(self, main_hi, incomplete_hi):
    self.main_hi = main_hi
    self.incomplete_hi = incomplete_hi
  
  def start(self):
    self.no_mining = False
  
  def readyGo(self, node):   # only called when self.mining_info is ['waiting']
    if self.no_mining: return False
    
    if time.time() - self.start_time >= MINING_AFTER_START:
      self.mining_info = None
      iTmp = self.main_hi - self.incomplete_hi
      if iTmp < 0 and len(node._incomp_blocks) == 0:
        self.state = 'mining'
        self.nextMining(node)
      else: self.state = 'sync'
      
      self.onStateChange(node,'waiting')
      return True
    
    return False
  
  def updateState(self, node, reason, new_hi, curr_hi):
    if self.no_mining: return
    
    if reason == 'new_header':  # curr_hi no use when reason is 'new_header'
      self.main_hi = new_hi
      
      info_ = self.mining_info
      if info_ and info_[0] == 'waiting': return
      
      if self.state == 'sync':
        pass   # state no change
      else:    # self.state is 'mining'
        if new_hi >= self.incomplete_hi:
          self.cancelMining('override')
          self.state = 'sync'
          self.onStateChange(node,'mining')
        # else, state no change
    
    elif reason == 'new_txns' or reason == 'no_header':
      if self.state == 'mining' and new_hi > self.main_hi:
        self.cancelMining('override')  # cancel old block mining
        self.state = 'sync'
        self.onStateChange(node,'mining')
      
      self.main_hi = new_hi
      self.incomplete_hi = curr_hi + 1
      
      info_ = self.mining_info
      if info_ and info_[0] == 'waiting': return
      
      if self.incomplete_hi <= new_hi:
        if self.state == 'mining':
          self.state = 'sync'
          self.onStateChange(node,'mining')
      else:  # self.incomplete_hi > new_hi, has catch up, start mining
        if self.state == 'sync':
          self.state = 'mining'
          self.onStateChange(node,'sync')
          self.nextMining(node)
    
    elif reason == 'win_compete':
      if self.state == 'mining' and new_hi == curr_hi:
        self.main_hi = new_hi
        self.incomplete_hi = curr_hi + 1
        self.nextMining(node)
      # else, ignore
  
  def onStateChange(self, node, old_state):
    if node.log_level <= node.LOG_LEVEL_INFO:
      print('mining pool state change: %s --> %s, %i: %i' % (old_state,self.state,self.main_hi,self.incomplete_hi))
  
  def nextMining(self, node):
    if self.no_mining: return
    
    try:
      height = self.main_hi
      info = node.prepare_new_header2(height)  # must be called in asyncore thread, db no changing during prepare
      if info: # (prev_time,prev_hash,bits,txns,header,new_time,link_no), header has exclude nonce
        _pow_thread.cancelCurr()
        _poet_thread.cancelCurr()
        
        header = info[4]
        self.mining_info = ['try_pow',info[1],height,info[0],info[2],info[3],header,info[5],info[6],_pow_thread.solvePow(header)]
    except:
      traceback.print_exc()
  
  def cancelMining(self, reason=''):
    if self.no_mining: return
    
    if _pow_thread: _pow_thread.cancelCurr()     # it is idempotence
    if _poet_thread: _poet_thread.cancelCurr()    # it is idempotence
    
    self._cancel_reason = reason
    self.mining_info = None
    
    if reason == 'socket_close':
      if _pow_thread: _pow_thread.exit()
      if _poet_thread: _poet_thread.exit()
  
  def change_to_sync(self, node, reason):
    if self.state == 'mining':
      self.cancelMining(reason)
      self.state = 'sync'
      self.onStateChange(node,'mining')
  
  def solvePoet(self, prev_hash, prev_hi, prev_time, new_bits, new_txns, new_header, new_hash, new_time, new_link): # new_header includes nonce
    if self.no_mining: return
    
    self.mining_info = ['try_poet',prev_hash,prev_hi,prev_time,new_hash,new_bits,new_txns,new_header,new_time,new_link]
    _poet_thread.appendTask(self.mining_info[1:])
  
  def getPowResult(self):
    return _pow_thread.getResult()
  
  def getPoetResult(self):
    return _poet_thread.getResult()

mine_pool = MinePool()

_pow_thread = consensus.PowThread()
_pow_thread.start()

#-------------

__LocalDbg__ = False

if not __LocalDbg__:
  import socket
  from .. import protocol
  
  def config_poet(node, port=None):
    global _poet_thread
    _poet_thread = PoetThread(node,port)
    _poet_thread.start()
    time.sleep(0.1)  # wait starting
  
  class PoetThread(Thread):
    POET_POOL_HEARTBEAT = 5               # heartbeat every 5 seconds
    
    def __init__(self, node, port=None):  # assert(not node._is_raw)
      Thread.__init__(self)
      self.daemon = True
      self._active = False
      self._cancelOne = False
      
      self._task_id = 0
      self._task = []
      self._outs = []
      
      self._node = node
      self._coin = node.coin
      self._address = (node.address[0],port or max(1000,(node.address[1]-1)))
      
      self.socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
      self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
      if hasattr(socket,'SO_REUSEPORT'):
        self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEPORT,1)
      self.socket.bind(self._address)
      self.socket.settimeout(self.POET_POOL_HEARTBEAT)
      
      self._recv_buffer   = b''
      self._last_rx_time  = 0
      self._reset_sock_tm = 0
    
    def exit(self):
      self._active = False
      self.join()
    
    def cancelCurr(self):
      self._cancelOne = True
      del self._task[:]
      del self._outs[:]
    
    def appendTask(self, task): # task: [prev_hash,prev_hi,prev_time,new_hash,new_bits,new_txns,new_header,new_time,new_link]
      del self._task[:]    # avoid adding two same task
      self._task_id = self._task_id + 1
      task.insert(0,self._task_id)
      self._task.append(task)
    
    def getResult(self):
      ret = (False, b'', b'', b'', b'')  # succ,prev_hash,new_header,new_miner,new_sig
      try:
        ret = self._outs.pop()
      except: pass
      return ret
    
    def run(self):
      self._active = True
      
      chunk = b''
      while self.is_alive() and self._active:
        if self._node._exited: break
        
        try:  # 1472 bytes(1500-8-20) or 548 bytes (576-8-20)
          chunk, addr = self.socket.recvfrom(1472)  # data in one UDP usually less than 1472
          self._recv_buffer  = chunk
          self._last_rx_time = time.time()
        except socket.timeout:
          chunk = b''
        except:           # meet unexpected error
          chunk = b''
          traceback.print_exc()
          
          if not self._reset_sock_tm:
            self._reset_sock_tm = time.time()
          time.sleep(self.POET_POOL_HEARTBEAT)  # avoid unexpected error
        
        if not chunk:
          if self._reset_sock_tm and time.time() - self._reset_sock_tm > 300: # more than 5 minutes when socket go bad
            self._reset_sock_tm = 0
            
            try:
              sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
              sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
              if hasattr(socket,'SO_REUSEPORT'):
                sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEPORT,1)
              sock.bind(self._address)
              sock.settimeout(self.POET_POOL_HEARTBEAT)
              
              self.socket.close()
              self.socket = sock
            except:
              traceback.print_exc()
          
          continue
        
        length = protocol.Message.first_msg_len(self._recv_buffer)  # length maybe None
        while length and length <= len(self._recv_buffer):
          data = self._recv_buffer[:length]
          try:
            msg = protocol.Message.parse(data,self._coin.magic)     # will check magic-code and sum-code
            self._recv_buffer = self._recv_buffer[length:]
            length = protocol.Message.first_msg_len(self._recv_buffer)  # next message length
            
            try:
              self.handle_message(data,msg,addr)  # hand one message that come from one peer
            except:
              traceback.print_exc()
          except protocol.UnknownMsgError as e:
            self._recv_buffer = b''       # avoid chaos
            self.invalid_command(data,e)
          except protocol.MsgFormatError as e:
            self._recv_buffer = b''
            self.invalid_command(data,e)
          except Exception as e:     # just print error, avoid stopping
            self._recv_buffer = b''
            print('receive meet error: %s' % e)
      
      try:
        self.socket.shutdown(2)
        self.socket.close()
      except: pass
      self._active = False
      print('PoET thread exited.')
    
    def handle_message(self, data, msg, peer_addr):
      logLevel = self._node.log_level
      if logLevel <= self._node.LOG_LEVEL_DEBUG:
        self._node.log('<<< ' + msg._debug(),peer=self._node,level=logLevel)
      
      sCmd = msg.command
      if sCmd == protocol.GetPoetTask.command:
        oneTask = None
        try:
          if self._task:
            oneTask = self._task[-1]
        except: pass
        
        if oneTask:
          sn = oneTask[0]
          if sn > msg.curr_id:
            new_hash = oneTask[4]
            new_bits = oneTask[5]
            new_num = len(oneTask[6])   # txn number
            new_hi = oneTask[2] + 1
            old_time = oneTask[3]
            new_time = oneTask[8]
            new_link = oneTask[9]
            
            new_msg = protocol.PoetInfo(new_link,sn,new_hash,new_bits,new_hi,old_time,new_time,new_num)
            self.send_message(new_msg,peer_addr)
          else:  # sn <= msg.curr_id, same task or older
            self.send_message(protocol.PoetReject(sn,msg.timestamp,'missed'),peer_addr)
        else:  # else, shakehands
          self.send_message(protocol.PoetReject(0,msg.timestamp,'invalid'),peer_addr)
      
      elif sCmd == protocol.PoetResult.command:
        oneTask = None
        try:
          if self._task:
            oneTask = self._task[-1]
        except: pass
        
        if oneTask and oneTask[0] == msg.curr_id:
          # avoid pseudo-message, verify PoET first
          if consensus.poet_verify(oneTask[4],oneTask[5],len(oneTask[6]),msg.miner,msg.sig_tee):
            prev_hash = oneTask[1]
            new_header = oneTask[7]
            
            del self._outs[:]             # should remove old result
            try:
              self._task.remove(oneTask)  # current task success finished
            except: pass
            self._outs.append((True,prev_hash,new_header,msg.miner,msg.sig_tee))
      
      elif sCmd == protocol.Ping.command:
        self.send_message(protocol.Pong(msg.nonce,self._node.link_no),peer_addr)
    
    def invalid_command(self, data, e):
      print('PoET service meet error: %s' % (e,))
    
    def send_message(self, msg, peer_addr):
      if not self._active: return  # ignore
      
      logLevel = self._node.log_level
      if logLevel <= self._node.LOG_LEVEL_DEBUG:
        self._node.log('>>> ' + msg._debug(),peer=self._node,level=logLevel)
      self.socket.sendto(msg.binary(self._coin.magic),peer_addr)

else:  # __LocalDbg__   # for debuging
  _startTime = time.time() - random.randint(0,4*3600-1)  # random within 4 hours
  
  MINER_ACCOUNT = None  # from wallet.Address(pub_key) or wallet.Address(priv_key)
  MINER_PUBADDR = b'\x00' * 32  # public key hash 
  
  DEBUG_FACTOR = 10     # try debase PoET difficulty, 1 = keep_same, bits * DEBUG_FACTOR
  
  _STANDARD_K      = 30000000
  _HALF_STANDARD_K = 15000000
  
  def config_poet(node, port=None):
    pass
  
  def config_debug(miner):
    global MINER_ACCOUNT, MINER_PUBADDR
    MINER_ACCOUNT = miner
    MINER_PUBADDR = util.key.publickey_to_hash(miner.publicKey())
  
  # expect_time = K / bits     # K = 30,000,000
  #   bits: 20(hard) ~ 10,000(easy)    # 10,000 for 10 nodes, expect time = 300 seconds
  #   time: 1,500,000 ~ 3,000 seconds
  def check_elapsed_time(block_hash, bits, txn_num, sig_flag=b'\x00', only_double=False):
    global _startTime
    
    bits = min(10000,max(20,bits))     #  20 <= bits <= 10000
    now = time.time()
    expect = _STANDARD_K // (bits * DEBUG_FACTOR)
    if now - _startTime >= expect:
      _startTime += expect
      lessExpect = int(0.99 * expect)
      if now >= _startTime + lessExpect:  # passed double-expect: 1.99 * expect
        _startTime = now - lessExpect     # max left less-expect time, avoid continous win
      elif only_double and bits >= 5000:  # bits >= 5000 means few miner
        _startTime -= expect              # reset _startTime
        return None                       # failed, but not reset _startTime
      
      return consensus.poet_sign(block_hash,bits,txn_num,sig_flag,MINER_PUBADDR)  # return PoET signature or None
    else: return None                     # failed
  
  def has_cooldown(lastTm, lastHi, nowHi, bits):
    if nowHi <= lastHi: return False
    
    half_expect = _HALF_STANDARD_K // (bits * DEBUG_FACTOR)
    if (nowHi - lastHi) * 300 > half_expect:
      return 1           # normal cooldown (half expected time)
    elif bits >= 5000:   # not so many peers
      now = time.time()
      if (now - lastTm) > (4 * half_expect):
        return 2         # has wait more than double expected time
    
    return 0             # not cooldown yet
  
  class PoetThreadDebug(Thread):
    def __init__(self):
      Thread.__init__(self)
      self.daemon = True
      self._active = False
      self._cancelOne = False
      
      self._task_id = 0
      self._task = []
      self._outs = []
    
    def exit(self):
      self._active = False
      self.join()    # wait forever
    
    def cancelCurr(self):
      self._cancelOne = True
      del self._task[:]
      del self._outs[:]
    
    def appendTask(self, task): # task: [prev_hash,prev_hi,prev_time,new_hash,new_bits,new_txns,new_header,new_time,new_link]
      del self._task[:]    # avoid adding two same task
      self._task_id = self._task_id + 1
      task.insert(0,self._task_id)
      self._task.append(task)
    
    def getResult(self):
      ret = (False, b'', b'', b'', b'')  # succ,prev_hash,new_header,new_miner,new_sig
      try:
        ret = self._outs.pop()
      except: pass
      return ret
    
    def run(self):
      self._active = True
      while self.is_alive() and self._active:  # waiting MINER_ACCOUNT ready
        if MINER_ACCOUNT: break
        time.sleep(1)
      
      while self.is_alive() and self._active:
        try:
          task_id,prev_hash,prev_hi,prev_time,new_hash,new_bits,new_txns,new_header,new_time,new_link = self._task.pop()
        except: prev_hash = None
        
        if prev_hash:
          self._cancelOne = False
          
          while self._active and not self._cancelOne:
            sig = check_elapsed_time(new_hash,new_bits,len(new_txns))
            if sig:
              del self._outs[:]   # should remove old result
              self._outs.append((True,prev_hash,new_header,MINER_PUBADDR,sig))
              break
            else: time.sleep(2)
        
        else: time.sleep(2)   # no PoET task, go next loop
      
      self._active = False
      print('PoET thread exited.')
  
  _poet_thread = PoetThreadDebug()
  _poet_thread.start()
