import sys, time, os, re, asyncore, random, base64, traceback

from binascii import hexlify, unhexlify
from threading import Event, Thread, Timer, current_thread

from .. import util
from .. import coins
from .. import protocol
from .. import script

from ..blockchain.unspent import Database as UnspentDB

from .basenode import BaseNode
from ..node import InvalidNatErr, AddrInUseErr, StopNode
from ..protocol import format

import boto3      # should run boto3 under python3.4+
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb',region_name=os.environ.get('AWS_REGION')) # region_name can be None
s3 = boto3.client('s3',aws_session_token=os.environ.get('AWS_ACCESS_KEY_ID'),region_name=os.environ.get('AWS_REGION'))

_s3db_bucket = os.environ.get('AWS_DB_BUCKET','nb-filedb')
_s3db_path = os.environ.get('AWS_DB_PATH','txn_files')

MAX_RELAY_COUNT = 100     # maximum recent messages a peer can ask to be relayed
RELAY_COUNT_DECAY = 10    # how many messages per second we forget from each peer

MIN_BYTES_PER_TXN = 100
MAX_BYTES_PER_TXN = 65536 # every txn can not large than 64 K

TX_MIN_FEE = 100                  # 0.00000100 NBC
TX_TRANSFER_MAX = 1000000000000   # 10000 NBC

CURR_NODE = None

_0 = b'\x00' * 32
_EMPTY_SIG   = b'\x00' * 110  # 1+72+1 + 1 + 33 + reserved(2)

_re_newline = re.compile(rb'\r\n|\n|\r')

def ORD(ch):      # compatible to python3
  return ch if type(ch) == int else ord(ch)

def CHR(i):       # compatible to python3
  return bytes(bytearray((i,)))

def _restore_s3_txns(block):
  b = []; meta = {}
  sPath = '%s/%i-%i.csv' % (_s3db_path,(block.lnk_id >> 32) & 0xffff,block.height),
  try:
    res = s3.Object(_s3db_bucket,sPath).get()
    if res['ResponseMetadata']['HTTPStatusCode'] == 200:
      meta = res['Metadata']
      b = _re_newline.split(res['Body'].read())
    else:    # try again
      time.sleep(2)
      res = s3.Object(_s3db_bucket,sPath).get()
      if res['ResponseMetadata']['HTTPStatusCode'] == 200:
        meta = res['Metadata']
        b = _re_newline.split(res['Body'].read())
      # else: b = []
  except: pass  # download failed, maybe file inexistent
  
  if not b: return None
  del b[0]       # remove field info line
  if len(b) < 2: return None     # at least have: header, one txn
  del b[0]       # remove block header line
  
  bk_id = int(meta.get('id',0))  # blockid >= 1
  bk_hash = unhexlify(meta.get('hash','00'))
  if bk_id != (block.lnk_id & 0xffffffff) or bk_hash != block.hash:
    return None  # download block mismatch
  
  # try get block txns
  txns = []
  for item in b:
    b2 = item.split(b',')
    if len(b2) == 3:
      txns.append(format.Txn.parse(base64.b64decode(b2[2]))[1])
  if block.txn_count != len(txns): return None
  
  return tuple(txns)

class Block_(object):
  def __init__(self, row):
    self.lnk_id = int(row['lnk_id_'])           # linkNo(16)+blockId(32)
    self.prev_lnk_id = int(row['prev_lnk_id_'])
    self.lnk_height = int(row['lnk_height_'])   # linkNo(16)+height(32)
    self.lnk_date = int(row['lnk_date_'])       # linkNo(16)+date(32)  # date is 31104000*N seconds
    
    self.hash = row['hash_'].value
    self.version = int(row['version_'])
    self.link_no = int(row['link_no_'])
    self.merkle_root = row['merkle_'].value
    self.timestamp = int(row['timestamp_'])
    self.bits = int(row['bits_'])
    self.nonce = int(row['nonce_'])
    self.miner = row['miner_'].value
    self.sig_tee = row['sig_tee_'].value
    self.txn_count = int(row['txn_count_'])
    self.txn_ready = int(row['txn_ready_'])
    self.mainchain = int(row['mainchain_'])
    self.miner_hint = int(row['miner_hint_'])
    
    self._txns = None
  
  @property
  def height(self):
    return self.lnk_height & 0xffffffff
  
  @property
  def _blockid(self):
    return self.lnk_id & 0xffffff   # 24 bits
  
  @property
  def previous_hash(self):
    prev = CURR_NODE.block_get(self.prev_lnk_id)
    if prev:
      return prev.hash
    else: return _0
  
  @property
  def txns(self):
    if not self.txn_ready: return ()
    
    if self._txns is None:
      try:
        self._txns = _restore_s3_txns(self)   # result maybe None
      except:
        traceback.print_exc()
    return self._txns  # maybe return None

def _fetch_txn_bin(csv_name, idx, check_hash=None):
  try:
    r = s3.select_object_content( Bucket = _s3db_bucket,
      Key = '%s/%s.csv' % (_s3db_path,csv_name),
      ExpressionType = 'SQL',
      Expression = "select * from s3object as s where s.index = '%i'" % (idx,),
      InputSerialization = {'CSV': {'FileHeaderInfo':'USE'}},
      OutputSerialization = {'CSV': {}} )
    
    for ev in r['Payload']:
      if 'Records' in ev:
        b = ev['Records']['Payload'].split(b',')          # payload is bytes
        if check_hash and check_hash != unhexlify(b[1]):  # ensure hash is same
          return None
        return base64.b64decode(b[2])
      # elif 'Stats' in ev: print(ev['Stats']['Details'])        
  except:
    traceback.print_exc()
  
  return None

class Transaction_(object):
  def __init__(self, row, ref_block=None):
    self.lnk_id = int(row['lnk_id_'])       # linkNo(16)+blockId(32)
    self.position = int(row['position_'])   # txns index of one block
    self.hash = row['hash_'].value
    self.mainchain = int(row['mainchain_'])
    self.height = int(row['height_'])
    
    self._txn = None
    self._txn_bin = None
    if ref_block:
      self._txn = ref_block.txns[self.position]
  
  @property
  def _blockid(self):
    return self.lnk_id & 0xffffff    # 24 bits
  
  @property
  def index(self):
    return self.position & 0xfffff   # 20 bits
  
  @property
  def txn_bin(self):
    if self._txn_bin is None:
      if self._txn:
        self._txn_bin = self._txn.binary()
        return self._txn_bin
      
      csv_name = '%i-%i' % ((self.lnk_id >> 32) & 0xffff, self.height)
      self._txn_bin = _fetch_txn_bin(csv_name,self.position)  # result maybe None
    
    return self._txn_bin    # maybe return None
  
  @property
  def txn(self):
    if self._txn is None:
      txn_bin = self.txn_bin
      if txn_bin:
        (_, self._txn) = protocol.Txn.parse(txn_bin)  # _ is length
    return self._txn        # maybe return None
  
  @property
  def outputs(self):
    return self.txn.tx_out  # self.txn maybe None

class Unspent_(object):
  def __init__(self, row):
    self.lnk_id = int(row['lnk_id_'])         # linkNo(16)+blockId(32)
    self.lnk_height = int(row['lnk_height_']) # linkNo(16)+height(32)  # height is 103680*N
    
    self.position = int(row['position_'])     # txnIndex(20)+outIndex(32)
    self.uock = int(row['uock_'])             # blockid(24)+txnIndex(20)+outIndex(20)
    self.height = int(row['height_'])
    self.addr = row['addr_'].value
    self.vcn = int(row['vcn_'])
    self.coin_type = row['coin_type_'].value
    self.value = int(row['value_'])
    
    self._txn = None
  
  @property
  def txn(self):
    if self._txn is None:
      csv_name = '%i-%i' % ((self.lnk_id >> 32) & 0xffff, self.height)
      txn_bin = _fetch_txn_bin(csv_name,(self.position >> 32) & 0xfffff)
      if txn_bin:  # maybe None
        (_, self._txn) = protocol.Txn.parse(txn_bin)  # _ is length
    return self._txn
  
  @property
  def pk_script(self):
    out_idx = self.position & 0xffffffff
    txn = self.txn
    return txn.tx_out[out_idx].pk_script if txn else None
  
  @property
  def txn_hash(self):
    txn = self.txn
    return txn.hash if txn else _0
  
  def get_txn(self):
    ret = self.txn
    if ret is None:
      pos_desc = '%i-%i-i' % ((self.lnk_id >> 32) & 0xffff, self.height, (self.position >> 32) & 0xfffff)
      raise Exception('transaction (%s) not ready' % (pos_desc,))
    else: return ret

class RawNode(BaseNode):
  _is_raw = True
  
  MAX_TRY_COUNT = 100
  
  MEMORY_POOL_SIZE = 30000         # maximum number of txns-pool
  MAX_TXNS_PER_BLOCK = 6000        # maximum number of txns in a block, suggest 5000
  SHEET_BUFFER_NUM = 256           # in processing txn-sheet number
  
  def __init__(self, data_dir=None, address=None, link_no=0, seek_peers=16, max_peers=125,log=sys.stdout, coin=coins.Newbitcoin):
    global CURR_NODE
    CURR_NODE = self
    
    if address is None: address = ('127.0.0.1',20303)
    BaseNode.__init__(self,data_dir,address,link_no,seek_peers,max_peers,log,coin)
    
    # relay_count maps peer to number of messages sent recently, so we can throttle peers when too chatty
    self._relay_count = dict()     # { peer:msg_num }
    self._last_relay_decay = time.time()
    
    self._mempool_idx = 0
    self._mempool = []       # circular buffer of MEMORY_POOL_SIZE most recent txns (in binary format)
    self._mempool_dict = {}
    self._mempool_sync = 3   # max query 3 peers for mempool-command when startup
    
    self._txns2relay = []
    
    # last time of remove invalid tx from _mempool
    self._rmv_bad_tx_tm = int(time.time()) - 2592000  # 2592000 means 30 days ago
    
    self.tb_blocks = dynamodb.Table('blocks')
    self.tb_txns   = dynamodb.Table('txns')
    self.tb_utxos  = dynamodb.Table('utxos')
    self.tb_states = dynamodb.Table('states')
    
    self._last_valid = 0
    self._last_hi = 0
    self._last_get_hi = 0
    
    self._apiserver_id = 0
    self._apiserver_cmd = []
    self._last_sheet = []
  
  def _safe_get(self, tb, **kwargs):
    tryNum = 0
    while True:
      try:
        res = tb.get_item(**kwargs)
        status = res.get('ResponseMetadata',{}).get('HTTPStatusCode',500)
        tryNum += 1
        
        if status >= 500:
          if tryNum > self.MAX_TRY_COUNT:
            print('fatal error: dynamodb meet max tries')
            return None        # error
        elif status == 200:
          return res.get('Item')
        
        time.sleep(min(tryNum,30))  # hold on and waiting network repaired
      except Exception as e:
        print('dynamo db error: ' + str(e))
        return None
  
  def _safe_query(self, tb, **kwargs):
    tryNum = 0
    while True:
      try:
        res = tb.query(**kwargs)
        status = res.get('ResponseMetadata',{}).get('HTTPStatusCode',500)
        tryNum += 1
        
        if status >= 500:
          if tryNum > self.MAX_TRY_COUNT:
            print('fatal error: dynamodb meet max tries')
            return None        # error
        elif status == 200:
          return res.get('Items')
        
        time.sleep(min(tryNum,30))  # hold on and waiting network repaired
      except Exception as e:
        print('dynamo db error: ' + str(e))
        return None
  
  def handle_close(self):
    BaseNode.handle_close(self)
  
  def query_link_mask(self):
    return None   # not support
  
  def serve_forever(self, db_backup=None):  # db_backup not used
    self._threadObj = current_thread()
    
    try:  # create DB object in asyncore thread
      Timer(60,self._prime_mempool).start()
      asyncore.loop(5,map=self)
    except StopNode as e:
      pass
    except Exception:
      traceback.print_exc()
    finally:
      self.handle_close()
  
  @property
  def valid_height(self):
    hi = self.chain_height    # try update
    return self._last_valid   # _last_valid is auto updated
  
  @property
  def chain_height(self):
    guest_hi = None
    
    now = int(time.time())
    if now - self._last_get_hi > 10:  # if less than 10 seconds, no need update
      self._last_get_hi = now
      
      m = now % 31104000   # 24 * 3600 * 360 = 31104000  # about one year
      tm = now - m
      param = { 'TableName':'blocks', 'IndexName':'lnk_date-lnk_height-index',
        'Limit':1, 'ScanIndexForward':False, 'ProjectionExpression':'lnk_height_',
        'KeyConditionExpression': Key('lnk_date_').eq((self._chain_no << 32) | tm),
      }
      res = self._safe_query(self.tb_blocks,**param)
      
      if not res and type(res) == list:  # not find in this period, try last period # seldom do this
        param['KeyConditionExpression'] = Key('lnk_date_').eq((self._chain_no << 32) | (tm-31104000))
        res = self._safe_query(self.tb_blocks,**param)
      
      if res:
        self._last_hi = guest_hi = int(res[0]['lnk_height_']) & 0xffffffff  # get low 32 bits
      # else, keep self._last_hi no change
    
    if guest_hi is not None:  # try update self._last_valid also
      m = guest_hi % 103680   # 12 * 24 * 360 = 103680, about one year, release 12 blocks per hour
      guest_hi -= m
      
      param = { 'TableName':'utxos', 'IndexName':'lnk_height-height-index',
        'Limit':1, 'ScanIndexForward':False, 'ProjectionExpression':'height_',
        'KeyConditionExpression': Key('lnk_height_').eq((self._chain_no << 32) | guest_hi),
      }
      res = self._safe_query(self.tb_utxos,**param)
      
      if not res and type(res) == list:  # not find in this period, try last period # seldom do this
        param['KeyConditionExpression'] = Key('lnk_height_').eq((self._chain_no << 32 ) | (guest_hi-103680))
        res = self._safe_query(self.tb_blocks,**param)
      
      if res:
        self._last_valid = int(res[0]['height_']) & 0xffffffff
    
    return self._last_hi
  
  def _prime_mempool(self):
    if self._exited: return   # avoid loop waiting
    if self._mempool_sync <= 0: return
    
    peers = [p for p in self.peers if p.verack]
    if not peers:
      Timer(30,self._prime_mempool).start()
      return
    
    # self._mempool_sync > 0, send mempool request at _mempool_sync is 3,2,1
    iLen = len(peers); iFrom = random.randint(0,iLen-1)
    iNum = min(iLen,self._mempool_sync)
    for i in range(iNum):
      peer = peers[(iFrom+i) % iLen]
      peer.send_message(protocol.MemoryPool(self._link_no))
      self._mempool_sync -= 1
    
    if self._mempool_sync > 0:
      Timer(30,self._prime_mempool).start()
  
  def _add_mempool(self, txn):  # _add_mempool() only run in asyncore thread
    idx = self._mempool_dict.get(txn.hash,-1)
    if idx >= 0: return  # already exists
    
    if len(self._mempool) >= self.MEMORY_POOL_SIZE:  # use as circular buffer
      oldTx = self._mempool[self._mempool_idx]       # oldTx maybe None
      self._mempool[self._mempool_idx] = txn
      if oldTx: self._mempool_dict.pop(oldTxn.hash,None)
      self._mempool_dict[txn.hash] = self._mempool_idx
      self._mempool_idx = (self._mempool_idx + 1) % self.MEMORY_POOL_SIZE
    else:
      self._mempool.append(txn)
      self._mempool_dict[txn.hash] = len(self._mempool)-1
  
  def _rmv_mempool(self, txid):
    idx = self._mempool_dict.pop(txid,-1)
    if idx >= 0:
      self._mempool[idx] = None
  
  def _search_mempool(self, txid):
    idx = self._mempool_dict.get(txid,-1)
    if idx >= 0:
      return self._mempool[idx]
    else: return None
  
  def disconnected(self, peer):
    BaseNode.disconnected(self,peer)
    self._relay_count.pop(peer,None)
  
  def process_sheet_(self, info2_):
    sTask = info2_[0]
    
    if sTask == 'relay_sheet':     # come from peer
      try:
        msg_, data_ = info2_[1:3]
        self._msg_ = msg_
        self._bytes_ = len(data_)
        self.command_transaction(None,msg_.version,msg_.tx_in,msg_.tx_out,msg_.lock_time,msg_.sig_raw)
      except:
        traceback.print_exc()
    
    elif sTask == 'make_sheet':    # only come from local
      (sn_, account, pay_to, submit, min_utxo, max_utxo) = info2_[1:7]
      try:
        pub_addr = account.address()
        (tx_sheet,pks_list,last_uocks) = self.make_txn_sheet([(0,pub_addr)],pay_to,None,min_utxo,max_utxo)  # limit=None, sort_flag=0, from_uocks=None
        if tx_sheet:
          tx_ins2 = []
          pks_list0 = pks_list[0]; pks_num = len(pks_list0)  # pks_num <= len(tx_sheet.tx_in)
          for (idx,tx_in) in enumerate(tx_sheet.tx_in):      # sign every inputs
            if idx < len(pks_list0):
              hash_type = 1
              payload = script.make_payload(pks_list0[idx],tx_sheet.version,tx_sheet.tx_in,tx_sheet.tx_out,0,idx,hash_type)
              sig = account.sign(payload) + CHR(hash_type)
              pub_key = account.publicKey()
              sig_script = CHR(len(sig)) + sig + CHR(len(pub_key)) + pub_key
              tx_ins2.append(protocol.TxnIn(tx_in.prev_output,sig_script,tx_in.sequence))
            else:
              tx_ins2.append(tx_in)   # waiting other people sign it
          
          sig_raw = tx_sheet.sig_raw  # wait to do: sign by RAW ...
          tx_sheet = protocol.Txn(tx_sheet.version,tx_ins2,tx_sheet.tx_out,tx_sheet.lock_time,sig_raw)
          self._last_sheet.append((sn_,tx_sheet))
          while len(self._last_sheet) > self.SHEET_BUFFER_NUM:
            del self._last_sheet[0]
          
          if submit:
            unsign_num = len(tx_ins2) - len(pks_list0)
            if unsign_num:
              raise Exception('some input not signed yet: %i' % unsign_num)
            
            self._add_mempool(tx_sheet)
            inv_msg = protocol.Inventory(self._link_no,[protocol.InventoryVector(protocol.OBJECT_TYPE_MSG_TX,tx_sheet.hash)])
            self.relay(inv_msg.binary(self.coin.magic),None)  # relay to all peers
      except:
        traceback.print_exc()
    
    elif sTask == 'echo': # for testing
      msg = info2_[1]
      print('echo> %s' % (msg,))
  
  def begin_loop(self):   # call from asyncore.loop(), every 5 seconds once
    if self._exited or self._mempool_sync > 0: return
    
    if self._txns2relay:
      try:
        while self._txns2relay:
          txn_hash,peer = self._txns2relay.pop()
          hashes = [txn_hash]
          
          msgMax = len(self._txns2relay) - 1
          while msgMax >= 0:
            txn_hash2,peer2 = self._txns2relay[msgMax]
            if peer == peer2:
              if self._mempool_dict.get(txn_hash2,-1) >= 0:  # still alive
                hashes.append(txn_hash2)   # join all txn-hash that come from same peer
              del self._txns2relay[msgMax]
            msgMax -= 1
          
          inv_msg = protocol.Inventory(self._link_no,[protocol.InventoryVector(protocol.OBJECT_TYPE_MSG_TX,hash2) for hash2 in hashes])
          self.relay(inv_msg.binary(self.coin.magic),peer)
      except:
        traceback.print_exc()
        self._txns2relay.clear()  # just clear to exclude unexpected txn
    
    info2_ = self._apiserver_cmd
    if not info2_: return
    
    try:  # no need protected by DbLocker.enter() and DbLocker.leave()
      info2_ = info2_.pop()
      if info2_: self.process_sheet_(info2_)
    except:
      traceback.print_exc()
  
  def relay(self, bin_msg, peer):   # relay message, providing when it has not reached quota
    if peer:
      if self._relay_count.get(peer,0) > MAX_RELAY_COUNT:  # quota reached
        return
      
      if peer not in self._relay_count:
        self._relay_count[peer] = 0
      self._relay_count[peer] += 1  # increase relay request
    
    for n in self.peers:
      if n == peer: continue
      try:
        n.send(bin_msg)    # relay message 
      except: pass         # ignore error
  
  def _decay_relay(self):  # aging policy for throttling relaying, called every 10 seconds
    now = time.time()
    
    dt = now - self._last_relay_decay
    for peer in list(self._relay_count):
      num = self._relay_count[peer]
      num -= dt * RELAY_COUNT_DECAY
      if num <= 0.0:
        self._relay_count.pop(peer,None)
      else:
        self._relay_cound[peer] = num
    
    self._last_relay_decay = now
  
  def heartbeat(self):
    if not BaseNode.heartbeat(self): return False
    
    if self._heartbeat_num % 6 == 1:    # call every 6 * HEARTBEAT_DELAY seconds
      self._decay_relay()        # give a little more room for relaying
    
    return True
  
  def block_get(self, lnk_id):
    param = {'Key':{'lnk_id_':lnk_id}}
    item = self._safe_get(self.tb_blocks,**param)
    if item:
      return Block_(item)
    else: return None
  
  def block_get_(self, txn):
    return self.block_get(txn.lnk_id)
  
  def txns_get(self, hash_, only_main=True):
    param = { 'TableName':'txns','IndexName':'hash-index',
      'KeyConditionExpression': Key('hash_').eq(hash_),
    }
    
    minId = self._chain_no << 32
    maxId = (self._chain_no + 1) << 32
    cond = Attr('lnk_id_').gte(minId) & Attr('lnk_id_').lt(maxId)
    if only_main:
      cond = cond & Attr('mainchain_').eq(1)
    param['FilterExpression'] = cond
    
    res = self._safe_query(self.tb_txns,**param)
    if res:
      return Transaction_(res[0])
    else: return None
  
  def utxo_get(self, lnk_id, position):
    param = {'Key':{'lnk_id_':lnk_id,'position_':position}}
    item = self._safe_get(self.tb_utxos,**param)
    if item:
      return Unspent_(item)
    else: return None
  
  def block_get2(self, hi):
    lnk_hi = (self._chain_no << 32) + hi
    param = { 'TableName':'blocks','IndexName':'lnk_height-index', 'Limit':1,
      'KeyConditionExpression': Key('lnk_height_').eq(lnk_hi),
    }
    res = self._safe_query(self.tb_blocks,**param)
    if res:
      return Block_(res[0])
    else: return None
  
  def block_get3(self, bk_hash):
    minId = self._chain_no << 32
    maxId = (self._chain_no + 1) << 32
    param = { 'TableName':'blocks','IndexName':'hash-index', 'Limit':1,
      'KeyConditionExpression': Key('hash_').eq(bk_hash),
      'FilterExpression': Attr('lnk_id_').gte(minId) & Attr('lnk_id_').lt(maxId),
    }
    res = self._safe_query(self.tb_blocks,**param)
    if res:
      return Block_(res[0])
    else: return None
  
  def _verify_txn(self, txn, byte_num):  # txn is format.Txn, and must not coinbase
    if not (MIN_BYTES_PER_TXN <= byte_num <= MAX_BYTES_PER_TXN):
      return False  # out of range
    if len(txn.tx_in) == 0: return False   # error: no input
    if len(txn.tx_out) == 0: return False  # error: no output
    return True
  
  def command_transaction(self, peer, version, tx_in, tx_out, lock_time, sig_raw):
    byte_num = self._bytes_ - 24  # 24 is prefix of payload
    txn = protocol.Txn(version,tx_in,tx_out,lock_time,sig_raw)
    txn_hash = txn.hash
    
    if self._mempool_dict.get(txn_hash,-1) == -1:    # inexistent yet
      old_txn = self.txns_get(txn_hash)
      if old_txn: return  # it is already used, just ignore
      
      # wait to do: verify RAW signature ...
      
      if not self._verify_txn(txn,byte_num): return  # verify not pass
      
      self._add_mempool(txn)
      if self._mempool_sync <= 0:  # _mempool has synchronized, need broadcast this transaction
        self._txns2relay.append((txn_hash,peer))
        # if len(self._txns2relay) > 10000: print('warning: too many txns to relay')
        # inv_msg = protocol.Inventory(self._link_no,[protocol.InventoryVector(protocol.OBJECT_TYPE_MSG_TX,txn_hash)])
        # self.relay(inv_msg.binary(self.coin.magic),peer)
  
  def command_get_data(self, peer, link_no, inventory):
    if self._link_no != link_no: return
    
    notfound = []; counter2 = 1000   # max send 1000 txns at one request
    for iv in inventory:     # look up each block and transaction
      if iv.object_type == protocol.OBJECT_TYPE_MSG_BLOCK:
        notfound.append(iv)  # no support block content query
      
      elif iv.object_type == protocol.OBJECT_TYPE_MSG_TX:
        if counter2 <= 0:
          notfound.append(iv)
          continue
        
        # search the memory pool and database
        txn = self._search_mempool(iv.hash)  # try find binary-txn in pool, it may take little time
        if not txn:
          tx = self.txns_get(iv.hash)
          if tx: txn = tx.txn
        
        if txn:  # found one
          peer.send_message(protocol.Transaction.from_txn(txn))
          counter2 -= 1
        else: notfound.append(iv)
    
    if notfound: # if anything not found
      peer.send_message(protocol.NotFound(self._link_no,notfound))
  
  def command_inventory(self, peer, link_no, inventory): # for RAW: only query memory pool's txns
    if self._link_no != link_no: return
    
    txns = []
    for iv in inventory:
      if iv.object_type == protocol.OBJECT_TYPE_MSG_TX:
        if not self._search_mempool(iv.hash):
          txns.append(iv)
    
    if txns:
      peer.send_message(protocol.GetData(self._link_no,txns))
  
  def command_memory_pool(self, peer, link_no):
    if self._link_no != link_no: return
    
    try:
      now = int(time.time())
      if now - self._rmv_bad_tx_tm > 3600:  # more than 1 hour
        year = now // 31536000  # 31536000 is seconds of 365 days
        if year != self._rmv_bad_tx_tm // 31536000:  # switch into new year
          tm = 0
        else: tm = self._rmv_bad_tx_tm
        self._rmv_bad_tx_tm = now
        
        res = self.tb_states.query( # use default H+R index
          KeyConditionExpression=Key('H').eq('RMV_TX#'+str(year)) & Key('R').gt(tm) )
        Items = res.get('Items')
        if Items:
          for row in Items:
            self._rmv_mempool(row['hash_'].value)
    except:
      traceback.print_exc()
    
    inv = [protocol.InventoryVector(protocol.OBJECT_TYPE_MSG_TX,t.hash) for t in self._mempool if t]
    peer.send_message(protocol.Inventory(self._link_no,inv))
  
  def command_version_ack(self, peer, link_no):
    BaseNode.command_version_ack(self,peer,link_no)
  
  def in_catching(self):
    hi = self.chain_height      # hi != 0 means self._blocks is ready
    hi2 = self.valid_height
    if hi and (hi - hi2) < 8:
      return False
    else: return True
  
  def list_unspent(self, coin_hash, limit=0, _min=0, _max=0, _from=0, _to=0):
    if not limit: limit = 512         # default search number of unspent
    minId = self._chain_no << 32
    maxId = (self._chain_no+1) << 32
    param = { 'TableName':'utxos','IndexName':'addr-uock-index','Limit':limit,
      'FilterExpression': Attr('lnk_id_').gte(minId) & Attr('lnk_id_').lt(maxId) }
    
    forward = True
    if _from and _from < 0:  # _from = -1 means decreasing
      _from = 0
      forward = False
    param['ScanIndexForward'] = forward
    
    bRet = []; query_num = 0
    while len(bRet) < limit:
      cond = Key('addr_').eq(coin_hash)
      if _from:
        if forward:
          cond = cond & Key('uock_').gt(_from)
        else: cond = cond & Key('uock_').lt(_from)
      if _to:
        cond = cond & Key('uock_').lte(_to)
      param['KeyConditionExpression'] = cond
      
      res = self._safe_query(self.tb_utxos,**param)
      if not res: break  # meet error, or there is no record
      query_num += 1
      
      b = [Unspent_(r) for r in res]
      if not b:
        _from = 0
      else: _from = b[-1].uock
      
      if _max:
        b = list(filter(lambda u: _min < u.value <= _max,b))
      elif _min:  # _min > 0
        b = list(filter(lambda u: _min < u.value,b))
      # else, _min = 0, no change
      
      for i in range(len(b)-1,-1,-1):
        u = b[i]
        if ((u.uock >> 20) & 0xfffff) == 0:  # is coinbase
          if self.chain_height - u.height < self.coin.COINBASE_MATURITY:  # not maturity yet
            del b[i]
      
      bRet.extend(b)
      if not _from: break
      if query_num >= 10: break  # not loop too much
    
    return bRet
  
  def list_unspent2(self, uocks, only_mature=True):
    bRet = []; lnk = self._chain_no
    for u in uocks:
      lnk_id = (lnk << 32) + (u >> 40)
      tx_idx = (u >> 20) & 0xfffff
      pos = (tx_idx << 32) + (u & 0xfffff)
      
      item = self.utxo_get(lnk_id,pos)
      if item:
        if only_mature and tx_idx == 0:  # it is coinbase
          if self.chain_height - item.height >= self.coin.COINBASE_MATURITY:
            bRet.append(item)
        else: bRet.append(item)
    
    return bRet
  
  def make_txn_sheet(self, pay_from, pay_to, limit=0, min_utxo=0, max_utxo=0, sort_flag=0, from_uocks=None):
    if not pay_from:
      raise Exception('no pay-from defined')
    if pay_from[0][0] != 0:  # value of first pay_from must fixed to 0
      raise Exception('invalid value of first pay-from')
    if not pay_to:
      raise Exception('no pay-to defined')
    
    pay_len = len(pay_from)
    if from_uocks is None:
      from_uocks = [0 for i in range(pay_len)]
    else:
      while len(from_uocks) < pay_len:
        from_uocks.append(0)
    
    ret_script = pay_to[0][1]  # must have pay_to[0]
    if len(pay_from) == 1 and len(pay_to) == 1 and pay_to[0][0] == 0 and ret_script[0:1] == b'\x6a':  # RETURN (0x6a) script
      if script.get_script_form(ret_script) != 'pay-to-return':
        raise Exception('invalid RETURN script')
      if not limit: limit = 32
    else:
      ret_script = ''   # is not RETURN
      if not limit: limit = 512
    
    pub_addr = None; expected_coin = None
    pub_hash = None     # vcn2 + hash32 + cointype
    other_pay = 0; other_ins = []; other_outs = []; other_pks = []
    
    i = 0
    utxos0 = []; last_uocks = [0]
    for (value,addr) in pay_from:  # pay_from = [(value,base58_address), ...]
      addr2 = util.base58.decode_check(addr)
      if not addr2 or len(addr2) < 36:  # ver1 + vcn2 + pub_hash32 + cointype
        raise Exception('invalid address: %s' % addr)
      coin_hash = addr2[3:]        # coin_hash: pub_hash32 + cointype
      
      # wait to do: check vcn2 ...
      
      # step 1: find other input's UTXO
      utxos = self.list_unspent(coin_hash,limit,min_utxo,max_utxo,from_uocks[i])
      if sort_flag != 0:  # sort by UTXO's value, low value will count first when it is sorted
        utxos.sort(key = lambda u: u.value)
      
      i += 1
      if i == 1:        # first item of pay_from
        pub_addr = addr
        pub_hash = addr2[1:]  # pub_hash must be assigned since len(pay_from) >= 1
        expected_coin = pub_hash[34:]
        utxos0 = utxos  # first pay_from's utxos
        continue
      
      if coin_hash[32:] != expected_coin:
        raise Exception('coin type (%s) mismatch to: %s' % (hexlify(coin_hash[32:]),hexlify(expected_coin)))
      
      other_pay += value
      
      # step 2: find others pk_script and setup TxnIn/TxnOut
      bIn = []; fees = 0; last_uock_ = 0
      for u in utxos:
        bIn.append(u)          # assert(u.pk_script is not None)
        if u.uock > last_uock_: last_uock_ = u.uock
        fees += u.value
        if fees >= value: break
      
      if fees < value:
        if len(bIn) >= limit:  # assert(len(utxos) == len(bIn))
          raise Exception('insufficient fee to pay (%s): too many dust output' % (addr,))
        else: raise Exception('insufficient fee to pay (%s)' % (addr,))
      
      last_uocks.append(last_uock_)
      other_pks.append([u.pk_script for u in bIn])
      other_ins.append([protocol.TxnIn(protocol.OutPoint(u.txn_hash,u.uock & 0xfffff),_EMPTY_SIG,0xffffffff) for u in bIn])
      # assert(len(other_pks[i-1]) == len(other_ins[i-1]))
      
      if fees > value:         # has charge
        hash_ = addr2[1:3] + coin_hash
        pk_script = b'\x76\xb8' + CHR(len(hash_)) + hash_ + b'\xb7\xac'  # pay to coin-public-key-hash # 0xb7 is OP_HASHVERIFY
        other_outs.append(protocol.TxnOut(fees-value,pk_script))
    
    # step 3: prepare payout list (public key hash), sum up pay also
    total = 0; payout_list = []
    if ret_script:    # is RETURN
      return_num = 1
      payout_list.append((0,pub_hash))  # output fixed to the account of pay_from[0]
    else:
      return_num = 0  # no RETURN script
      for (value,addr) in pay_to:               # pay_to = [(value,base58_addr), ...]
        total += value
        addr2 = util.base58.decode_check(addr)
        if not addr2 or len(addr2) < 36:        # ver1 + vcn2 + keyhash32 + cointype
          raise Exception('invalid address: %s' % addr)
        
        # wait to do: check vcn2 ...
        
        hash_ = addr2[1:]
        if hash_[34:] != expected_coin:
          raise Exception('coin type (%s) mismatch to: %s' % (hexlify(hash_[34:]),hexlify(expected_coin)))
        payout_list.append((value,hash_))   # vcn2 + hash32 + cointype
    
    # step 4: try setup original sheet
    tx_sheet = None; pks_list = None; last_uock_ = 0
    fee_times = 1
    for i in range(10):     # max try 10 times
      # expected = total + (fee_times + return_num) * UnspentDB.TX_MIN_FEE - other_pay
      expected = total + fee_times * UnspentDB.TX_MIN_FEE - other_pay  # modified at 2019.01.03
      
      fees = 0; bIn = []; last_uock_ = 0
      for u in utxos0:
        bIn.append(u)       # assert(u.pk_script is not None)
        if u.uock > last_uock_: last_uock_ = u.uock
        fees += u.value
        if fees >= expected: break
      
      if fees < expected:
        if len(utxos0) >= limit:  # assert(len(utxos0) == len(bIn))
          raise Exception('insufficient fee to pay (%s): too many dust output' % (pub_addr,))
        else: raise Exception('insufficient fee to pay (%s)' % (pub_addr,))
      
      pks_list = [u.pk_script for u in bIn]
      tx_ins  = [protocol.TxnIn(protocol.OutPoint(u.txn_hash,u.uock & 0xfffff),_EMPTY_SIG,0xffffffff) for u in bIn]
      for b in other_ins: tx_ins.extend(b)
      
      tx_outs = []
      if ret_script:
        tx_outs.append(protocol.TxnOut(0,ret_script))
      else:
        for (value_,hash_) in payout_list:  # hash_ is: vcn2 + hash32 + cointype
          pk_script = b'\x76\xb8' + CHR(len(hash_)) + hash_ + b'\xb7\xac'  # pay to coin-public-key-hash  # 0xb7 is OP_HASHVERIFY
          tx_outs.append(protocol.TxnOut(value,pk_script))
      tx_outs.extend(other_outs)  # charge of other input
      
      if fees > expected:
        pk_script = b'\x76\xb8' + CHR(len(pub_hash)) + pub_hash + b'\xb7\xac'  # pay to coin-public-key-hash  # 0xb7 is OP_HASHVERIFY
        tx_outs.append(protocol.TxnOut(fees - expected,pk_script))  # charge of pay_from[0]
      
      txn_item = protocol.Txn(1,tx_ins,tx_outs,0,b'')  # sig_raw = b''
      (n,q) = divmod(len(txn_item.binary()),1024)
      if q: n += 1
      
      if n > fee_times:
        if n > 64: raise Exception('transaction size out of range') # max size: 64K
        fee_times = n             # continue next loop
      else:
        tx_sheet = txn_item
        break
    
    last_uocks[0] = last_uock_
    other_pks.insert(0,pks_list)  # make all pk_script list
    return (tx_sheet,other_pks,last_uocks) # items of other_pks must have same order of items in tx_sheet.tx_in
  
  def make_sheet(self, account, pay_to, submit=True, min_utxo=0, max_utxo=0):
    self._apiserver_id = sn = self._apiserver_id + 1
    self._apiserver_cmd.insert(0,('make_sheet',sn,account,pay_to,submit,min_utxo,max_utxo))
    return sn
  
  def get_sheet(self, sn=None):
    if self._last_sheet:
      if sn is None:       # means get last one, (sn,sheet)
        return self._last_sheet[-1]
      for item in self._last_sheet:
        if item[0] == sn:
          return item      # return (sn,txn)
    return (0,None)        # sn=0 means not found
  
  def txn_state(self, sn=0, txn=None):
    if not txn:
      if self._last_sheet:
        if sn:
          for item in self._last_sheet:
            if item[0] == sn:
              txn = item[1]
              break
        else: txn = self._last_sheet[-1][1]  # sn == 0 means take last txn
    if not txn:
      return 'no transaction sheet'
    
    txn2 = self.txns_get(txn.hash)
    if txn2:
      block = self.block_get(lnk_id)
      if block and block.mainchain:
        return 'transaction has accepted, confirm = %i' % (self.chain_height - block.height,)
    
    return 'transaction not accept yet'
  
  #----- ignore following process ----
  
  def command_block(self, peer, version, link_no, prev_block, merkle_root, timestamp, bits, nonce, miner, sig_tee, txns):
    if self._link_no != link_no:
      self.punish_peer(peer,'not in same link')
      return
  
  def command_get_blocks(self, peer, version, link_no, block_locator_hashes, hash_stop):
    if self._link_no != link_no: return
  
  def command_get_headers(self, peer, version, link_no, block_locator_hashes, hash_stop):
    if self._link_no != link_no: return
  
  def command_not_found(self, peer, link_no, inventory):
    pass  # do nothing
  
  def command_headers(self, peer, headers):
    pass
