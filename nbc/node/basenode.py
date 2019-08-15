
import sys, time, os, re, asyncore, socket, random, struct, traceback
from binascii import hexlify, unhexlify

from functools import cmp_to_key
from threading import Event, Thread, Timer, current_thread

import six

from .. import util
from .. import coins
from .. import protocol
from .. import blockchain
from .. import script

from . import connection
from . import mining

from ..util import pynat
from ..node import AddrInUseErr, StopNode, InvalidNatErr, init_upnp, add_portmap, remove_portmap
from ..protocol import format as _format

from ..blockchain import InvalidBlockException, DbLocker
from ..blockchain import unspent as bc_unspent
from ..blockchain import keys as bc_keys
from ..blockchain import database as bc_database
from ..blockchain import transaction as bc_transaction

if six.PY3:
  xrange = range  # range is same to xrange in python3

_list_types = [tuple,list]

def ORD(ch):      # compatible to python3
  return ch if type(ch) == int else ord(ch)

def CHR(i):       # compatible to python3
  return bytes(bytearray((i,)))

VERSION = [0,0,1]

_0 = b'\x00' * 32         # 32 bytes buffer

MAX_ADDRESSES = 2500      # maximum number of address stored in memory
ADDRESSES_PER_ASK = 1000  # maximun number of address returned when ask by peer

MAX_RELAY_COUNT = 100     # maximum recent messages a peer can ask to be relayed
RELAY_COUNT_DECAY = 10    # how many messages per second we forget from each peer

HEARTBEAT_DELAY = 10      # check it every 10 seconds

NODE_ACCESS_ADDR = None   # ('127.0.0.1',30303)

_EMPTY_SIG   = b'\x00' * 110  # 1+72+1 + 1 + 33 + reserved(2)

NAT_OPEN = 0
NAT_UPNP = 1
NAT_FULL = 2              # FULL_CONE
NAT_RESTRICTED1 = 3       # RESTRICTED_CONE
NAT_RESTRICTED2 = 4       # RESTRICTED_PORT
NAT_SYMMETRIC = 5
NAT_UNKNOWN = 6

IN_CLOUD_ENV = 'ENV_cloud' in os.environ

def get_ip_info3():       # try 3 times
  def tryOne(notLast=True):
    try:
      info = pynat.get_ip_info(include_internal=True)
      if notLast and info[0] == pynat.BLOCKED: return None
      return info
    except (socket.error, pynat.PynatError):
      if notLast:
        return None
      else: raise
  return tryOne() or tryOne() or tryOne(False)

def getNatInfo(port, coin):
  natType, extIp, extPort, intIp = get_ip_info3()
  if natType in (pynat.BLOCKED,pynat.UDP_FIREWALL):
    natType = NAT_UNKNOWN
  elif natType == pynat.OPEN:
    natType = NAT_OPEN
  elif natType == pynat.FULL_CONE:
    natType = NAT_FULL
  elif natType == pynat.RESTRICTED_CONE:
    if IN_CLOUD_ENV:
      natType = NAT_FULL  # take as NAT_FULL when in AWS clould
    else: natType = NAT_RESTRICTED1
  elif natType == pynat.RESTRICTED_PORT:
    if IN_CLOUD_ENV:
      natType = NAT_FULL  # take as NAT_FULL when in AWS clould
    else: natType = NAT_RESTRICTED2
  elif natType == pynat.SYMMETRIC:
    natType = NAT_SYMMETRIC
  else:
    natType = NAT_UNKNOWN
  
  upnpPort = 0
  if natType != NAT_UNKNOWN and natType != NAT_OPEN and natType != NAT_FULL:
    upnp = init_upnp()
    if upnp:   # support upnp
      try:
        upnp.selectigd() # select Internet Gateway Device
        upnpInfo = add_portmap(port,'TCP',coin.name)
        if upnpInfo:     # upnp behind NAT (upnp,external_ip,external_port,internal_ip)
          upnpPort = upnpInfo[2] # need call remove_portmap(upnpInfo[4],'TCP')
          if upnpInfo[1] == extIp:
            natType = NAT_UPNP
          # else, upnp under NAT
      except Exception as e:
        print('map upnp meet error: %s' % e)
  
  return (natType,extIp,extPort,intIp,upnpPort)

_RE_IPV4 = re.compile('^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')

def get_raw_domain(raw_seed, chain_no):
  ip = raw_seed[0]
  sre = _RE_IPV4.match(ip)
  if sre:
    return (sre.string,raw_seed[1])
  
  domain = ip.replace('%',str(chain_no))
  seedInfo = socket.getaddrinfo(domain,raw_seed[1],socket.AF_UNSPEC,socket.SOCK_STREAM)
  if seedInfo: # af, socktype, proto, canonname, addr = seedInfo[0]
    return seedInfo[0][4]
  else: raise InvalidNatErr('invalid seed: ' + domain)

class BaseNode(asyncore.dispatcher, object):
  LOG_LEVEL_PROTOCOL = 0
  LOG_LEVEL_DEBUG    = 1
  LOG_LEVEL_INFO     = 2
  LOG_LEVEL_ERROR    = 3
  LOG_LEVEL_FATAL    = 4
  
  _chain_no  = 0
  _link_mask = 0     # total link number is: _link_mask + 1
  
  _is_raw = False
  
  def __init__(self, data_dir=None, address=None, link_no=0, seek_peers=16, max_peers=125, log=sys.stdout, coin=coins.Newbitcoin):
    BaseNode._link_mask = (link_no >> 16) & 0xffff       # high 16 bit is link_mask
    BaseNode._chain_no  = link_no & BaseNode._link_mask  # low 16 bits is link no
    
    asyncore.dispatcher.__init__(self,map=self)
    self._link_no = link_no
    self._coin = coin
    
    if data_dir is None:
      data_dir = util.default_data_dir()
    self._data_dir = data_dir
    
    self._threadObj = None
    self._threadDeep = 0
    
    self._peers = dict()
    self._addresses = dict() # map of (address,port) to (timestamp,service)
    
    self._seek_peers = seek_peers
    self._max_peers = max_peers
    self._log = log
    self._log_level = self.LOG_LEVEL_ERROR
    
    self._peer_alias = {}
    self._raw_alias  = {}
    
    self._bootstrap = None
    if not self._is_raw:
      self._bootstrap = get_raw_domain(coin.raw_seed,self._chain_no)
    
    self._banned = dict()
    self._user_agent = '/nbc:%s(%s)/' % ('.'.join(str(i) for i in VERSION),coin.name)
    self._last_heartbeat = 0
    self._heartbeat_num = 0
    
    self._exited = False
    self._bytes_ = 0
    self._msg_ = None
    self._tx_bytes = 0    # total send bytes
    self._rx_bytes = 0    # total receive bytes
    
    self._listen = True
    if address is None:
      self._listen = False
      address = ('0.0.0.0',30303)
    if not address[1]: raise InvalidNatErr('invalid port')
    
    self._nat_info = None
    self._fix_addr = None    # RAW listen address
    if self._is_raw:
      _, extIp, _, intIp = get_ip_info3()
      self._fix_addr = (extIp,address[1])
      if address[0] == '0.0.0.0': address = (intIp,address[1])
    else:
      if not self._bootstrap: raise InvalidNatErr('no bootstrap')
      if NODE_ACCESS_ADDR is None:
        if IN_CLOUD_ENV:
          _, extIp, _, intIp = get_ip_info3()
          if address[0] == '0.0.0.0': address = (intIp,address[1])
          self._nat_info = (NAT_FULL,extIp,address[1],intIp,0)
        else:
          nat_info = getNatInfo(address[1],coin)
          #if nat_info[0] not in (NAT_OPEN,NAT_UPNP,NAT_FULL):  # only support OPEN NAT_FULL and one NAT level's UPNP
          #  raise InvalidNatErr('invalid UPNP behind NAT')
          self._nat_info = nat_info
          if address[0] == '0.0.0.0': address = (nat_info[3],address[1])
      else:
        if address[0] == '0.0.0.0': address = (pynat.get_internal_ip(),address[1])
        self._nat_info = (NAT_OPEN,) + NODE_ACCESS_ADDR + (address[0],0)
    self._address = address
    
    try:
      nat_info = getNatInfo(address[1],coin)
      
      self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
      self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
      if hasattr(socket,'SO_REUSEPORT'):
        self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEPORT,1)
      self.bind(address)
      self.listen(5)
    except socket.error as e:  # port in use... Maybe already running
      if e.errno == 48:
        raise AddrInUseErr()
      raise e
  
  link_no = property(lambda s: s._link_no)  # 0x link_mask(16) link_no(16)
  coin = property(lambda s: s._coin)
  data_dir = property(lambda s: s._data_dir)
  
  # blockchain height will include when connecting to a peer, sub-class should override it
  chain_height = 0
  
  address = property(lambda s: s._address)
  ip = property(lambda s: s._address[0])
  port = property(lambda s: s._address[1])
  nat_info = property(lambda s: s._nat_info)
  
  @property
  def ext_addr(self):
    if self._nat_info:
      return (self._nat_info[1],self._nat_info[2])
    elif self._is_raw:
      return self._fix_addr
    else: return None   # invalid address
  
  def _get_user_agent(self):
    return self._user_agent
  def _set_user_agent(self, user_agent):
    self._user_agent = user_agent
  user_agent = property(_get_user_agent,_set_user_agent)
  
  def _get_log_level(self):
    return self._log_level
  def _set_log_level(self, log_level):
    self._log_level = log_level
  log_level = property(_get_log_level,_set_log_level)
  
  def log(self, message, peer=None, level=LOG_LEVEL_INFO):
    if self._log is None or level < self._log_level: return
    
    if peer:
      source = peer.address[0]
      port = ':' + str(peer.address[1])
    else:
      source = 'node'
      port = ''
    message = '(%s%s) %s' % (source,port,message)
    six.print_(message,file=self._log)
  
  def serve_forever(self, db_backup=None):
    self._threadObj = current_thread()
    
    try:
      asyncore.loop(5,map=self)
    except StopNode as e:
      pass
    finally:
      self.handle_close()
  
  def handle_close(self):
    try:
      self.close()
    except Exception as e:
      pass
    
    port = self._nat_info and self._nat_info[4]
    if port: # has upnp port mapping
      self._nat_info = None
      remove_portmap(port,'TCP')
    self._exited = True
    print('main node closed.')
  
  def close(self):
    peers = self.peers
    try:
      for peer in peers:
        peer.shutdown(2)
        peer.close()
    except: pass
    
    time.sleep(0.1)
    asyncore.dispatcher.close(self)
  
  def invalid_command(self, peer, payload, exception):
    self.log('invalid command: len=%i %r (%s)' % (len(payload),payload[:160],exception))
  
  def is_connected(self, peer):
    pass  # do nothing, called when know peer version
  
  def disconnected(self, peer):   # called when peer closed
    addr = (peer._peer_ip, peer._peer_port)
    self.log('connection closed: ' + str(addr),level=self.LOG_LEVEL_DEBUG)
    self._addresses.pop(addr,None)
  
  def add_peer(self, address, force=True):
    peers = self.peers
    if not force and len(peers) >= self._max_peers:
      return False  # too much peers and not force to do
    
    actives = [(n._peer_ip,n._peer_port) for n in peers]
    if address in actives:
      return False  # already exists this peer
    
    ret = True
    try:
      connection.Connection(node=self,address=address)  # asyncore will keep a reference
    except Exception as e:
      print('connect meet error: %s' % e)
      traceback.print_exc()
      ret = False
    return ret
  
  def remove_peer(self, address):
    peers = self.peers
    for peer in peers:
      if peer.address == address:
        peer.handle_close()
        break
  
  @property
  def peers(self):
    return [n for n in self.values() if isinstance(n,connection.Connection)]
  
  #----------------
  
  def punish_peer(self, peer, reason=None):
    peer.add_banscore()
    if peer.banscore > 5:
      self._banned[(peer._peer_ip,peer._peer_port)] = time.time()
      self.log('close connection because of ban',level=self.LOG_LEVEL_ERROR)
      peer.handle_close()
  
  def add_any_peer(self, tried_peer=[]):
    extAddr = self.ext_addr
    if not extAddr: return None   # something wrong
    
    peers = self.peers
    actives = [(n._peer_ip,n._peer_port) for n in peers]
    actives.append(extAddr)       # can not connect to self
    for (k,v) in self._peer_alias.items():
      if v in actives:            # if same peer exists, add it's alias addr
        actives.append(k)
      elif k in actives:
        actives.append(v)
    
    addrLen = len(self._addresses)
    exist_raw = sum([(1 if k in self._raw_alias else 0) for k in actives])
    if addrLen == 0 or (exist_raw == 0 and random.randint(0,9 if addrLen < 16 else 90) == 1): # if no address, or for sometimes use dns seeds
      addr = None
      if not self._is_raw:
        try:           # connecting to RAW would be more common than normal
          self._bootstrap = addr = get_raw_domain(self._coin.raw_seed,self._chain_no)  # update ip address
          self._raw_alias[addr] = True
        except: pass   # ignore any error
      
      if addr and (addr not in actives) and (addr not in tried_peer) and (addr not in self._banned):
        self.add_peer(addr,force=False)
        return addr
    else:
      items = list(self._addresses.items())
      for item in items:
        addr = item[0]
        if addr[0] in ('0.0.0.0','127.0.0.1'): continue
        if addr in actives: continue   # ignore already connected one
        if addr in self._raw_alias: continue
        if addr in tried_peer: continue
        if addr in self._banned: continue
        
        self.add_peer(addr)    # try add one peer
        return addr
    
    return None
  
  def heartbeat(self):         # called every HEARTBEAT_DELAY seconds for maintenance
    try:    # avoid thread death
      self._heartbeat_num += 1
      
      # try remove old expired item of self._addresses
      if self._is_raw:
        expiredSec = 7200      # remove address that income at 2 hours ago
      else: expiredSec = 7200  # remove address that income at 2 hours ago
      if (self._heartbeat_num % 128) == 1:  # abount every 20 minutes
        peers = self.peers
        
        now = int(time.time())
        actives = [(n._peer_ip,n._peer_port) for n in peers]
        items = list(self._addresses.items())
        for item in items:     # self._addresses maybe changed when in iteration
          addr = item[0]
          passed = now - item[1][0]
          if passed >= expiredSec and addr not in actives:
            if passed >= 86400:                # remove too old address (before 24 hours)
              self._addresses.pop(addr,None)
            elif len(self._addresses) > 10:    # try keep some when len <= 10, delay removing after 24 hours
              self._addresses.pop(addr,None)
        
        if (self._heartbeat_num % 65536) == 1: # about every 7 days
          self._peer_alias.clear()
          self._raw_alias.clear()
      
      if (self._heartbeat_num % 6) == 1:       # about every 1 minutes
        peers = self.peers
        
        # if need more peer connections, attempt to add some
        if not self._is_raw:
          triedPeer = []
          for i in range(0,min(self._seek_peers-len(peers),3)): # max 3 at a time
            item = self.add_any_peer(triedPeer)
            if item: triedPeer.append(item)
        
        # if not many addresses, try ask more
        if peers and len(self._addresses) < 20:
          peer = random.choice(peers)
          peer.send_message(protocol.GetAddress(self._link_no))
        
        # give a little back to peers that went bad but seem be OK now
        for peer in peers:
          peer.reduce_banscore()
      
      return True
    except:
      self.log(traceback.format_exc(),level=self.LOG_LEVEL_ERROR)
      return False
  
  #----------------
  
  def begin_loop(self):   # activated in asyncore loop, will override in sub-class
    pass
  
  def handle_accept(self):
    pair = self.accept()
    if not pair: return
    
    (sock,address) = pair
    print('Incoming connection from %s' % repr(address))
    
    if address in self._banned: # it is no use, address usually changed by NAT
      if time.time() - self._banned[address] < 3600:
        sock.close()            # banned it within one hour
        return
      self._banned.pop(address,None) # else, ignore early banned list
    
    if not self._listen:        # if not accepting incoming, drop it
      sock.close()
      return
    
    connection.Connection(node=self,sock=sock,address=address) # asyncore will keep a reference in map for us
  
  def readable(self):
    return True
  
  # emulate a dictionary so we an pass in the Node as the asyncode map
  
  def items(self):  # map.items() will be called in asyncore loop
    if self._threadObj == current_thread():
      self._threadDeep += 1
      if self._threadDeep == 1:  # avoid recursive enter
        self.begin_loop()
        
        now = time.time()
        if now - self._last_heartbeat > HEARTBEAT_DELAY: # more than HEARTBEAT_DELAY seconds since last heartbeat
          self._last_heartbeat = now
          self.heartbeat()
      
      self._threadDeep -= 1
    
    return list(self._peers.items())
  
  def values(self):
    return list(self._peers.values())
  
  def keys(self):
    return list(self._peers.keys())
  
  def get(self, name, default = None):
    return self._peers.get(name,default)
  
  def __nonzero__(self):
    return True
  
  def __len__(self):
    return len(self._peers)
  
  def __getitem__(self, name):
    return self._peers[name]
  
  def __setitem__(self, name, value):
    self._peers[name] = value
  
  def __delitem__(self, name):
    del self._peers[name]
  
  def __iter__(self):
    return iter(self._peers)
  
  def __contains__(self, name):
    return name in self._peers
  
  #------ command_xxx ----
  
  def command_ping(self, peer, nonce):
    peer.send_message(protocol.Pong(nonce,self._link_no))
  
  def command_pong(self, peer, nonce, link_no):
    pass
  
  def command_version(self, peer, version, link_no, services, timestamp, addr_recv, addr_from, nonce, user_agent, start_height, relay):
    # if self._link_no != link_no: return  # has checked in connection.handle_message()
    peer.send_message(protocol.VersionAck(self._link_no)) # it always feedback self link_no
  
  def command_version_ack(self, peer, link_no): # a peer acknowledged us, record address
    if self._link_no != link_no: return
    if len(self._addresses) < MAX_ADDRESSES:
      self._addresses[(peer._peer_ip,peer._peer_port)] = (int(peer._last_rx_time),peer.services)
  
  def command_get_address(self, peer, link_no):
    if self._link_no != link_no: return
    
    def sortAddr(a,b): # by timestamp and with decrease
      return self._addresses[b][0] - self._addresses[a][0]
    
    addresses = []
    for address in sorted(self._addresses,key=cmp_to_key(sortAddr)):  # add the most recent peers
      (timestamp, services) = self._addresses[address]
      if services is None: continue
      (ip, port) = address
      if ip == '0.0.0.0' or ip == '127.0.0.1': continue
      
      addresses.append(protocol.NetworkAddress(timestamp,services,ip,port))
      if len(addresses) >= ADDRESSES_PER_ASK: break
    
    peer.send_message(protocol.Address(self._link_no,addresses)) # send our address list to peer
  
  def command_address(self, peer, link_no, addr_list):
    if self._link_no != link_no: return
    
    extAddr = self.ext_addr
    for addr in addr_list:
      if len(self._addresses) > MAX_ADDRESSES: return
      addr2 = (addr.address,addr.port)
      if addr2 != extAddr:
        self._addresses[addr2] = (addr.timestamp,addr.services)

class SyncNode(BaseNode):
  TOP_INCOMPLETE_INFLIGHT = 5000   # maximum of pending getdata requests for a peer
  MAX_INCOMPLETE_INFLIGHT = 1000   # the peer will change to candidate if too many block pending
  BLOCKS_PER_ONE_INFLIGHT = 50     # max blocks in one getdata request, 50 blocks should replied within 10 minutes
  
  MAX_INCOMPLETE_BLOCKS = 10000    # maximum of incomplete block (self._incomp_blocks) to be tracked
  MAX_INCOMPLETE_FETCH  = 2000     # maximum of incomplete block for one db-query
  
  MEMORY_POOL_SIZE = 30000         # maximum number of txns-pool
  
  LOW_ORPHAN_SIZE = 36
  MAX_ORPHAN_SIZE = 72  # will remove older items when more than MAX_ORPHAN_SIZE, resize to LOW_ORPHAN_SIZE
  
  MAX_TXNS_PER_BLOCK = 6000        # maximum number of txns in a block, suggest 5000
  
  SHEET_BUFFER_NUM = 256           # in processing txn-sheet number
  
  def __init__(self, data_dir=None, address=None, link_no=0, seek_peers=16, max_peers=125,log=sys.stdout, coin=coins.Newbitcoin):
    BaseNode.__init__(self,data_dir,address,link_no,seek_peers,max_peers,log,coin)
    
    # relay_count maps peer to number of messages sent recently, so we can throttle peers when too chatty
    self._relay_count = dict()     # { peer:msg_num }
    self._last_relay_decay = time.time()
    
    self._blocks  = None  # blockchain.block.Database()
    self._txns    = None  # self._blocks._txns
    self._unspent = None  # self._blocks._unspent
    
    self._mempool_idx = 0
    self._mempool = []       # circular buffer of MEMORY_POOL_SIZE most recent txns (in binary format)
    self._mempool_dict = {}
    self._mempool_sync = 3   # max query 3 peers for mempool-command when startup
    
    self._txns2relay = []
    
    self._mine_pool = mining.mine_pool
    
    self._last_query_time = 0        # last interactive with others
    self._last_get_headers = 0       # how long since last asked for headers or blocks
    
    self._incomp_blocks = dict()     # { blockhash:last_request_time }
    self._last_incomp   = None
    
    self._inflight_blocks = dict()   # { peer:number_in_process }
    self._inflight_headers = dict()  # { peer:last_time }
    
    self._orphans = {}               # { blockhash: Block }
    
    self._apiserver_id = 0
    self._apiserver_cmd = []
    self._last_sheet = []
  
  def handle_close(self):
    self._mine_pool.cancelMining('socket_close')
    BaseNode.handle_close(self)
  
  def query_link_mask(self):
    if self._is_raw: return None
    
    ret = None
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    
    try:
      sock.settimeout(20)
      raw_addr = get_raw_domain(self._coin.raw_seed,0)
      sock.connect((raw_addr[0],self._coin.raw_seed[1]))
      
      data = sock.recv(1024)
      length = protocol.Message.first_msg_len(data)
      if length and length <= len(data):
        msg = protocol.Message.parse(data[:length],self.coin.magic)
        if msg.command == protocol.Version.command:
          ret = (msg.link_no >> 16) & 0xffff
    except: pass
    
    sock.close()
    return ret
  
  def serve_forever(self, db_backup=None):
    self._threadObj = current_thread()
    
    try:  # create DB object in asyncore thread
      if not self._is_raw:
        link_mask = self.query_link_mask()
        if link_mask is None: link_mask = self.query_link_mask()
        if link_mask is None:
          raise Exception('query link mask failed')
        else: self._link_mask = link_mask
      
      self._blocks = blockchain.block.Database(self._data_dir,self._coin)
      self._blocks.gen_linkno_table(self._chain_no,self._link_mask)
      
      self._txns = self._blocks._txns
      self._unspent = self._blocks._unspent
      
      if db_backup:
        db_backup.init_db(self)
        while db_backup._state != 'cloud_sync':  # try backup db first, maybe waiting long time
          time.sleep(5)
          print('local valid=%i, backup in %s' % (self._unspent._last_valid.height,db_backup.state()))
        print('load block from existing finished, local valid: %i' % (self._unspent._last_valid.height,))
        
        if db_backup.readonly: db_backup.exit()  # only dump from cloud db, no backup
      
      if not self._is_raw: mining.config_poet(self)
      self._mine_pool.config(self.chain_height,self._unspent._last_valid.height+1)
      
      Timer(60,self._prime_mempool).start()
      asyncore.loop(5,map=self)
    except StopNode as e:
      pass
    except Exception:
      traceback.print_exc()
    finally:
      if self._blocks:
        self._blocks.close()  # safe close database
      self.handle_close()
  
  @property
  def chain_height(self):
    if self._blocks is None:  # not use bool(self._blocks) that will call __len__(_blocks)
      return 0
    return self._blocks._topmost.height
  
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
    self._inflight_blocks.pop(peer,None)
  
  def prepare_new_header2(self, top_height):
    top_block = self._blocks._topmost
    if top_block.height != top_height:
      return None
    
    # step 1: caculate bits
    now = int(time.time())
    bits = top_block.bits
    next_hi = top_height + 1
    if next_hi % 504 == 0:
      pre_period = self._blocks._get2(next_hi-504)
      if pre_period and pre_period.bits == bits:
        standardTm = 120960 + 12.0 * bits   # is float  # 504 * (240 + bits/42.0))
        new_ = int(bits * ((now - pre_period.timestamp) / standardTm))  # use new bits
        new_ = min(bits*4,max(bits//4,new_))
        bits = min(10000,max(20,new_))
      else: return None  # failed
    
    # step 2: prepare txns
    txns = []; using_uock = {}
    large_txn_num = 0; fees = 0
    max_bkid = self._unspent._last_valid._blockid - 4 # position that supposed stable
    for txn_ in self._mempool:
      if not txn_: continue
      
      # validate 1: txn_ has been used?
      txn2_ = self._txns.get(txn_.hash)
      if txn2_:
        if txn2_._blockid < max_bkid:
          self._rmv_mempool(txn_.hash)   # history stable txn, just remove from pool
        continue  # it is used txn, just ignore
      
      txn_ok = True; fee_get = 0
      
      # validate 2: exceed max size?
      txn_size = len(txn_.binary())
      if txn_size > self._txns.MAX_BYTES_PER_TXN:
        print('exceed max bytes, ignore txn: %s' % hexlify(txn_.hash))
        txn_ok = False
      
      curr_coin = None
      if txn_ok:
        fee_in = 0
        fee_out = sum(item.value for item in txn_.tx_out)
        
        uock_dict = {}
        for tx_in in txn_.tx_in:
          # validate 3: exists tx_in UTXO?
          po = tx_in.prev_output
          prev_txn = self._txns.get(po.hash)
          if not prev_txn:
            print('try spend inexistent utxo, ignore txn: %s' % hexlify(txn_.hash))
            txn_ok = False
            break
          
          # validate 4: coinbase of tx_in maturity?
          if bc_keys.get_txck_index(prev_txn._txck) == 0: # coinbase
            block2_ = self._blocks._get(prev_txn._blockid)
            if not block2_ or (next_hi - block2_.height) < self.coin.COINBASE_MATURITY:
              print('coinbase not maturity, ignore txn: %s' % hexlify(txn_.hash))
              txn_ok = False     # not maturity yet
              break
          
          # validate 5: is standard txn?
          poItem = prev_txn.outputs[po.index]
          sf = script.get_script_form(poItem.pk_script)
          if sf == 'non-standard':
            print('using none-standard UTXO, ignore txn: %s' % hexlify(txn_.hash))
            txn_ok = False       # only support standard script form
            break
          
          # validate 6: double spend utxo?
          uock = bc_keys.get_uock(prev_txn._txck,po.index)
          if using_uock.get(uock) or uock_dict.get(uock):
            txn_ok = False       # meet error, double spend
            break
          if not self._unspent.exist_uock(uock):
            txn_ok = False       # meet error, spend inexistent uxto
            break
          uock_dict[uock] = True
          
          # validate 7: same input coin type?
          coin_type_ = script.get_script_cointype(poItem.pk_script,self.coin)
          if not coin_type_:
            txn_ok = False       # meet error
            break
          if curr_coin is None:
            curr_coin = coin_type_
          else:
            if coin_type_ != curr_coin:
              txn_ok = False     # meet error
              break
          
          fee_in += poItem.value
        
        # join uock_dict to using_uock
        if txn_ok:
          using_uock.update(uock_dict)
      
      # validate 8: same output coin type?
      if not curr_coin:
        txn_ok = False
      elif txn_ok:
        for item in txn_.tx_out:
          if item.value == 0 and item.pk_script[0:1] == b'\x6a':  # is OP_RETURN
            continue
          if curr_coin != script.get_script_cointype(item.pk_script,self.coin):
            txn_ok = False   # error: coin type of output mismatch
            break
      
      # validate 9: insuffient fee?
      if txn_ok:
        (n, q) = divmod(txn_size,1024)
        if q: n += 1
        fee_get = fee_in - fee_out
        if fee_get < n * self._unspent.TX_MIN_FEE:  # modified at 2019.01.03, no special for RETURN
          print('insuffient fee, ignore txn: %s' % hexlify(txn_.hash))
          txn_ok = False
      
      # validate 10: all input 'sig_script+pk_script' OK?
      if txn_ok:
        try:
          # txck=txn_index (not 0), txid_hint=0, mainchain=1
          pseudoTxn = bc_transaction.Transaction(self._txns,(len(txns)+1,0,1,txn_),txn_)
          txio = script.Script(pseudoTxn,next_hi,self.coin)
          if not txio.verify(self._blocks): # verify all input signature
            print('prepare txn (hi=%i) verification failed: %s' % (next_hi,hexlify(txn_.hash)))
            txn_ok = False
        except:
          traceback.print_exc()
          txn_ok = False
      
      if not txn_ok:  # invalid txn
        self._rmv_mempool(txn_.hash)
        bc_database.db_backup.add_task(('-pool',txn_.hash))
        continue
      
      #------- current txn is OK -------
      fees += fee_get
      txns.append(txn_)
      if txn_size > 2048:
        large_txn_num += 1
      if large_txn_num >= 1000 or len(txns) >= (self.MAX_TXNS_PER_BLOCK-1): break
    
    # step 3: insert coinbase txn
    rewards = bc_unspent.get_block_subsidy(next_hi)
    txnIn = protocol.TxnIn(protocol.OutPoint(b'\x00'*32,0xffffffff),struct.pack('<BI',4,next_hi),0xffffffff) # sig_script: push 4 bytes (block height)
    txnOut1 = protocol.TxnOut(rewards,b'\x76\xb8\xb9\x88\xac')  # mining rewards, DUP OP_HASH512 OP_MINERHASH OP_EQUALVERIFY OP_CHECKSIG # 0xb8 is OP_HASH512(OP_NOP9), 0xb9 is OP_MINERHASH(OP_NOP10)
    txnOut2 = protocol.TxnOut(fees,b'\x76\xb8\xb9\x88\xac')     # mining fees
    txns.insert(0,protocol.Txn(1,[txnIn],[txnOut1,txnOut2],0xffffffff,b'')) # sig_raw = b''
    
    # step 4: caculate merkle_root and binary block header
    merkle_root = util.get_merkle_root(txns)
    msg = util.get_block_header2( self.coin.block_version,self._link_no,top_block.hash,
            merkle_root,now,bits )
    
    return (top_block.timestamp,top_block.hash,bits,txns,msg,now,self._link_no)
  
  def process_mining_(self, info_):
    sTask = info_[0]
    
    if sTask == 'waiting':
      if len(self.peers) >= 1:  # at least have 1 peers than can relay blocks
        self._mine_pool.readyGo(self)
    
    elif sTask == 'try_pow':    # ['try_pow', prev_hash,prev_hi,prev_time, bits,txns,header,new_time,link_no,idx]
      prevHash = info_[1]
      if prevHash != self._blocks._topmost.hash:
        self._mine_pool.change_to_sync(self,'abandon')
      else:
        succ, idx2, nonce, hash2 = self._mine_pool.getPowResult()
        if succ:
          prevHi, prevTime, bits, txns, header, newTime, newLink, idx = info_[2:10]
          self._mine_pool.mining_info = None
          if idx2 == idx and (time.time() - self._last_query_time) < 600: # 600 means has interact with other node within last 10 minutes
            self._mine_pool.solvePoet(prevHash,prevHi,prevTime,bits,txns,header+struct.pack('<I',nonce),hash2,newTime,newLink)
          # else, target block or PoW has shift or current node in isolated, just ignore
        # else, PoW not finished yet, try again next loop
    
    elif sTask == 'try_poet':   # ['try_poet', prev_hash,prev_hi,prev_time, hash,bits,txns,header]
      prevHash = info_[1]
      if prevHash != self._blocks._topmost.hash:
        self._mine_pool.change_to_sync(self,'abandon2')
      else:
        msg_hash, bits, txns, header = info_[4:8]  # header includes nonce here
        succ, prevHash2, header2, miner2, sig2 = self._mine_pool.getPoetResult()
        if succ:  # consensus.poet_verify() has passed
          self._mine_pool.mining_info = None
          
          processed = False; curr_block = None
          if prevHash == prevHash2 and id(header) == id(header2):
            (ver4,link2,prev32,merkle32,time4,bits4,nonce4) = struct.unpack('<II32s32sIII',header)
            # msg_block = protocol.Block(ver4,link2,prev32,merkle32,time4,bits4,nonce4,miner2,sig2,txns)
            bk_header = _format.BlockHeader(ver4,link2,prev32,merkle32,time4,bits4,nonce4,miner2,sig2,len(txns))
            
            try:
              curr_block = None
              try:
                # curr_block = self._blocks.add_header(self,msg_block.make_header(),False,msg_hash) # False means no-check-duplicate
                curr_block = self._blocks.add_header(self,bk_header,False,msg_hash) # False means no-check-duplicate
                self._txns.add(self,curr_block,txns) # add transactions
              except InvalidBlockException as e:
                if curr_block:
                  if curr_block.mainchain:
                    self._mine_pool.cancelMining('invalid_block')  # just for safty, since the top of blockchain changed
                  self._blocks.reject_block(curr_block)
                raise e;
              
              if curr_block:
                if curr_block.mainchain:
                  curr_block = self._unspent.catch_up_check(self,curr_block)
                hi = self.chain_height
                if hi == curr_block.height and msg_hash == curr_block.hash:
                  tm = time.localtime()
                  self.log('mining successful (%i:%i): %i' % (tm.tm_hour,tm.tm_min,hi),level=self.LOG_LEVEL_INFO)
                  
                  self._mine_pool.cancelMining('success')  # set _mine_pool.mining_info to None
                  self._mine_pool.updateState(self,'new_txns',hi,curr_block.height)
                  processed = True
                  
                  # self.relay(msg_block.binary(self.coin.magic),None) # not relay block directly, relay 'Inventory' instead
                  inv_msg = protocol.Inventory(self._link_no,[protocol.InventoryVector(protocol.OBJECT_TYPE_MSG_BLOCK,msg_hash)])
                  self.relay(inv_msg.binary(self.coin.magic),None)     # None means relay to all peer
            except:
              traceback.print_exc()
          
          if not processed:
            self._mine_pool.change_to_sync(self,'error')
        # else, PoET not finished yet, try again next loop
  
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
    
    info_ = self._mine_pool.mining_info
    info2_ = self._apiserver_cmd
    if info2_:
      try:
        info2_ = info2_.pop()
      except: info2_ = None
    
    if info_ or info2_:
      DbLocker.enter()
      try:
        if info_:
          self.process_mining_(info_)
        if info2_:
          self.process_sheet_(info2_)
      except: pass
      DbLocker.leave()
  
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
    
    if self._heartbeat_num % 6 == 1:    # about every 1 minute
      self._decay_relay()        # give a little more room for relaying
    
    try:   # avoid thread death
      if len(self._peers) > 1:   # one of _peers is self node  # if have any peers, poke them to sync blocks
        self.sync_blockchain_headers()
        self.sync_blockchain_blocks()
      return True
    except:
      traceback.print_exc()
      return False
  
  def cache_orphan(self,header_hash, msg_block):
    self._orphans[header_hash] = msg_block
    rmv_num = len(self._orphans) - self.MAX_ORPHAN_SIZE
    if rmv_num > 0:  # too many orphan in cache, try remove some
      rmv_num += (self.MAX_ORPHAN_SIZE - self.LOW_ORPHAN_SIZE)
      b = [(b.timestamp,h) for (h,b) in self._orphans.items()]  # no height property, so we have to remove by timestamp
      b.sort()
      for (_, h) in b[:rmv_num]:
        self._orphans.pop(h,None)
  
  def _check_can_mining(self, peer, msg_hash, msg_block):
    add2Top = False; waitAdd = []; curr_block = None
    
    try:
      preceed_block = None
      
      sHash = msg_block.prev_block
      while True:
        tmpBlock = self._orphans.get(sHash)
        if tmpBlock:
          waitAdd.insert(0,(sHash,tmpBlock))
          sHash = tmpBlock.prev_block  # continue next loop
        else:
          preceed_block = self._blocks.get(sHash,orphans=True)
          break
      
      if not preceed_block:
        self.cache_orphan(msg_hash,msg_block)
        del waitAdd[:]     # set len(waitAdd) == 0
      else:
        waitAdd.append((msg_hash,msg_block))
        for (hash_,block) in waitAdd: # block is protocol.Block
          curr_block = self._blocks.get(hash_,orphans=True)
          if not curr_block:
            curr_block = self._blocks.add_header(self,block.make_header(),False,hash_) # False means no-check-duplicate
            # assert(curr_block)      # no-check-duplicate also means curr_block must exists
            add2Top = True
          
          self._txns.add(self,curr_block,block.txns)  # add transactions, curr_block.txns will be set
          
          preceed_block = curr_block  # curr_block.txn_ready/mainchain has corrected
          self._orphans.pop(hash_,None)
    
    except InvalidBlockException as e:
      if curr_block:
        if curr_block.mainchain:
          self._mine_pool.cancelMining('invalid_block')  # just for safty, since the top of blockchain changed
        self._blocks.reject_block(curr_block)
      raise e;
    
    if waitAdd and curr_block:  # when one or more block added, try catch up
      curr_block = self._unspent.catch_up_check(self,curr_block)
      hi = self.chain_height
      if hi == curr_block.height and add2Top and curr_block.mainchain and msg_hash == curr_block.hash:
        inv = [protocol.InventoryVector(protocol.OBJECT_TYPE_MSG_BLOCK,msg_hash)]
        self.relay(protocol.Inventory(self._link_no,inv).binary(self.coin.magic),peer) # only after catching up then relay newest block
      
      self._mine_pool.updateState(self,'new_txns',hi,curr_block.height)
  
  def _check_next_sync(self):
    if len(self._orphans) == 0: return
    
    b = self._blocks.incomplete_blocks(max_count=self.MAX_INCOMPLETE_FETCH)
    if not b: return
    
    self._last_incomp = b[-1]
    for block in b:
      hash_ = block.hash
      msg_block = self._orphans.pop(hash_,None)
      if msg_block:
        try:
          self._txns.add(self,block,msg_block.txns)   # add transactions
        except InvalidBlockException as e:
          self.log('invalid block: %s' % e,level=self.LOG_LEVEL_DEBUG)
          self._incomp_blocks[hash_] = 0       # wait to try again
        except:  # unknown error, just ignore
          traceback.print_exc()
      else:
        self._incomp_blocks[hash_] = 0         # last request time is 0, more than 10 minutes ago
  
  def command_block(self, peer, version, link_no, prev_block, merkle_root, timestamp, bits, nonce, miner, sig_tee, txns):
    if self._link_no != link_no:
      self.punish_peer(peer,'not in same link')
      return
    
    self._last_query_time = time.time()
    
    if len(txns) > self.MAX_TXNS_PER_BLOCK:
      self.punish_peer(peer,'exceed max transactions of block')
      return
    
    if peer in self._inflight_blocks:
      n = self._inflight_blocks[peer]
      if n > 0:
        self._inflight_blocks[peer] = n - 1  # has use one quantity
    
    header = util.get_block_header(version,link_no,prev_block,merkle_root,timestamp,bits,nonce)
    header_hash = util.sha256d(header)
    new_block_ = None
    
    try:
      if self._incomp_blocks.get(header_hash):     # in sync block state
        self._incomp_blocks.pop(header_hash,None)  # avoid retry
        
        curr_block = self._blocks.get(header_hash,orphans=True)
        if not curr_block:
          return  # not raise InvalidBlockException()  # maybe just reject_block
        
        try:
          self._txns.add(self,curr_block,txns)     # add transactions
        except InvalidBlockException as e:
          if curr_block:
            if curr_block.mainchain:
              self._mine_pool.cancelMining('invalid_block')  # just for safty, since the top of blockchain changed
            self._blocks.reject_block(curr_block)
          raise e;
        
        if curr_block.mainchain:
          curr_block = self._unspent.catch_up_check(self,curr_block)
        
        self._orphans.pop(header_hash,None)
        
        if curr_block and curr_block.mainchain:
          self._mine_pool.updateState(self,'new_txns',self.chain_height,curr_block.height)
      
      else:
        if prev_block == self._blocks._topmost.hash:
          new_block_ = self._blocks.add_header(self,self._msg_.make_header(),False,header_hash) # False means no-check-duplicate
        # else, new_block_ is None
        
        if len(self._incomp_blocks) or self._last_incomp is None:  # hash incomp_blocks or just call reset_last_incomp()
          self.cache_orphan(header_hash,self._msg_)
        else:
          if self._unspent._last_valid.height < self._blocks._topmost.height-16:  # still in sync block state
            self.cache_orphan(header_hash,self._msg_)
          else:  # not in sync block
            self._check_can_mining(peer,header_hash,self._msg_)  # try using _orphans and catch up deep checking
          
          if new_block_:
            inv_msg = protocol.Inventory(self._link_no,[protocol.InventoryVector(protocol.OBJECT_TYPE_MSG_BLOCK,new_block_.hash)])
            self.relay(inv_msg.binary(self.coin.magic),peer)
      
      if len(self._incomp_blocks) == 0:
        self._check_next_sync()
    except InvalidBlockException as e:
      self.log('invalid block: %s' % e,level=self.LOG_LEVEL_DEBUG)
      if new_block_: self._blocks.reject_block(new_block_)       # avoid block-hanging
      self.punish_peer(peer,str(e))
    
    except:
      if new_block_: self._blocks.reject_block(new_block_)       # avoid block-hanging
      traceback.print_exc()
  
  def command_get_blocks(self, peer, version, link_no, block_locator_hashes, hash_stop):
    if self._link_no != link_no: return
    
    blocks = self._blocks.locate_blocks(block_locator_hashes,500,hash_stop)
    if blocks:  # find any block
      inv = [protocol.InventoryVector(protocol.OBJECT_TYPE_MSG_BLOCK,b.hash) for b in blocks]
      self.send_message(protocol.Inventory(self._link_no,inv))
    else:       # nothing match
      self.send_message(protocol.NotFound(self._link_no,block_locator_hashes))
  
  def _verify_txn(self, txn, byte_num):    # txn must not coinbase
    _txns = self._txns
    if not (_txns.MIN_BYTES_PER_TXN <= byte_num <= _txns.MAX_BYTES_PER_TXN):
      return False  # out of range
    if len(txn.tx_in) == 0: return False   # error: no input
    if len(txn.tx_out) == 0: return False  # error: no output
    return True
  
  def command_transaction(self, peer, version, tx_in, tx_out, lock_time, sig_raw):
    self._last_query_time = time.time()
    
    byte_num = self._bytes_ - 24  # 24 is prefix of payload
    txn = protocol.Txn(version,tx_in,tx_out,lock_time,sig_raw)
    txn_hash = txn.hash
    
    if self._mempool_dict.get(txn_hash,-1) == -1:    # inexistent yet
      old_txn = self._txns.get(txn_hash)
      if old_txn: return  # it is already used, just ignore
      
      # wait to do: verify RAW signature ...
      
      if not self._verify_txn(txn,byte_num): return  # verify not pass
      
      self._add_mempool(txn)
      if self._mempool_sync == 0:  # _mempool has synchronized, need broadcast this transaction
        self._txns2relay.append((txn_hash,peer))
        # if len(self._txns2relay) > 10000: print('warning: too many txns to relay')
        # inv_msg = protocol.Inventory(self._link_no,[protocol.InventoryVector(protocol.OBJECT_TYPE_MSG_TX,txn_hash)])
        # self.relay(inv_msg.binary(self.coin.magic),peer)
  
  def command_get_data(self, peer, link_no, inventory):
    if self._link_no != link_no: return
    
    self._last_query_time = time.time()
    
    notfound = []; counter1 = 50; counter2 = 1000   # max send 50 blocks or 1000 txns at one request
    for iv in inventory:  # look up each block and transaction
      if iv.object_type == protocol.OBJECT_TYPE_MSG_BLOCK:
        if counter1 <= 0:
          notfound.append(iv)
          continue
        
        msg_block = self._orphans.get(iv.hash)  # find from _orphans first, it should more quick than DB
        if msg_block:
          peer.send_message(msg_block)
          counter1 -= 1
        else:
          block = self._blocks.get(iv.hash)
          if block and block.txn_ready: # return one ready-block when it exists in local database
            peer.send_message(protocol.Block.from_block(block))  # not include transation
            counter1 -= 1
          else: notfound.append(iv)
      
      elif iv.object_type == protocol.OBJECT_TYPE_MSG_TX:
        if counter2 <= 0:
          notfound.append(iv)
          continue
        
        # search the memory pool and database
        txn = self._search_mempool(iv.hash)  # try find binary-txn in pool, it may take little time
        if not txn:
          tx = self._txns.get(iv.hash,only_main=False)
          if tx: txn = tx.txn
        
        if txn:  # found one
          peer.send_message(protocol.Transaction.from_txn(txn))
          counter2 -= 1
        else: notfound.append(iv)
    
    if notfound: # if anything not found
      peer.send_message(protocol.NotFound(self._link_no,notfound))
  
  def command_get_headers(self, peer, version, link_no, block_locator_hashes, hash_stop):
    if self._link_no != link_no: return
    
    self._last_query_time = time.time()
    
    # peer has block_locator_hashes=[hash...], try get newer 2000 blocks, 2000 blocks is about 7 days
    blocks = self._blocks.locate_blocks(block_locator_hashes,2000,hash_stop) # maybe []
    peer.send_message(protocol.Headers([protocol.BlockHeader.from_block(b) for b in blocks]))
  
  def command_headers(self, peer, headers):
    self._inflight_headers.pop(peer,None)  # no longer get_header in-flight for this peer
    if len(headers) == 0:
      hi = self._unspent._last_valid.height
      if hi == self.chain_height:
        self._mine_pool.updateState(self,'no_header',hi,hi)
      return
    
    # add headers to database (we fill in the transactions later)
    add_count = 0
    try:
      for header in headers:  # maybe take long time (8M)  # len(headers) can be 2000
        curr_bk = self._blocks.add_header(self,header)     # check_exist = True
        if curr_bk:
          add_count += 1
        else:
          self.log('block header already exists: %s' % hexlify(header.hash),level=self.LOG_LEVEL_DEBUG)
    except InvalidBlockException as e:
      self.log('invalid block header: (%s)' % (e,),level=self.LOG_LEVEL_DEBUG)
      self.punish_peer(peer,str(e))
    
    if add_count:  # newly add some header
      new_hi = self.chain_height
      self._mine_pool.updateState(self,'new_header',new_hi,new_hi)
    
    # we got some headers, so can request next batch now, random choose peer
    self.sync_blockchain_headers(new_headers=add_count>0)
  
  def command_inventory(self, peer, link_no, inventory):
    if self._link_no != link_no: return
    
    blocks = []; txns = []
    for iv in inventory:
      if iv.object_type == protocol.OBJECT_TYPE_MSG_BLOCK:
        if not self._orphans.get(iv.hash) and not self._blocks.get(iv.hash,orphans=True):
          blocks.append(iv)
      elif iv.object_type == protocol.OBJECT_TYPE_MSG_TX:
        if not self._search_mempool(iv.hash):
          txns.append(iv)
    
    if blocks:  # should get data of some block that inexistent yet
      peer.send_message(protocol.GetData(self._link_no,blocks))
    if txns:
      peer.send_message(protocol.GetData(self._link_no,txns))
  
  def command_memory_pool(self, peer, link_no):
    if self._link_no != link_no: return
    
    inv = [protocol.InventoryVector(protocol.OBJECT_TYPE_MSG_TX,t.hash) for t in self._mempool if t]
    peer.send_message(protocol.Inventory(self._link_no,inv))
  
  def command_not_found(self, peer, link_no, inventory):
    if self._link_no != link_no: return
    
    blocks = [b.hash for b in inventory if b.object_type == protocol.OBJECT_TYPE_MSG_BLOCK]
    if blocks:
      if peer in self._inflight_blocks:
        leftNum = self._inflight_blocks[peer] - len(blocks)  # the peer haven't some block
        if leftNum < 0: leftNum = 0
        self._inflight_blocks[peer] = leftNum
      
      for h in blocks:
        if h in self._incomp_blocks:
          self._incomp_blocks[h] = 0    # let other peer can quickly try the block again
  
  def command_version_ack(self, peer, link_no):
    if self._link_no != link_no:
      if not self._is_raw:
        try:
          link_mask = self.query_link_mask()  # maybe link_mask has changed
          if (link_mask is not None) and link_mask != self._link_mask:
            self._link_mask = link_mask
            return
        except: pass
      
      self.punish_peer(peer,'not in same link')
      return
    
    BaseNode.command_version_ack(self,peer,link_no)  # self._link_no == link_no
    
    self.sync_blockchain_headers()
    self.sync_blockchain_blocks()
  
  def in_catching(self):
    if self._incomp_blocks: return True  # still in sync
    
    hi = self.chain_height      # hi != 0 means self._blocks is ready
    if hi and (hi - self._unspent._last_valid.height) < 8:
      return False
    else: return True
  
  def reset_last_incomp(self):  # any mainchain block switch to none-mainchain
    self._incomp_blocks.clear()
    self._last_incomp = None    # next will search from beginning
  
  def sync_blockchain_headers(self, new_headers=False):
    now = time.time()
    if not new_headers and now - self._last_get_headers < 30:
      return  # normally respond to gettheaders every 30 seconds
    self._last_get_headers = now
    
    # assume they forgot
    for peer in list(self._inflight_headers):
      if now - self._inflight_headers[peer] > 900:   # after 15 minutes
        self._inflight_headers.pop(peer,None)
        self.punish_peer(peer,'no response for get_headers')
    
    # random pick a ready peer that has received VersionAck
    peers = [p for p in self.peers if (p.verack and (p not in self._inflight_headers))]
    if not peers: return
    peer = random.choice(peers)
    self._inflight_headers[peer] = now
    
    # request the next block headers (if any)
    locator = self._blocks.block_locator_hashes() # when for just-started node, locator = [coin.genesis_block_hash]
    getheaders = protocol.GetHeaders(self.coin.protocol_version,self._link_no,locator,_0)
    peer.send_message(getheaders)
  
  def sync_blockchain_blocks(self):
    # try add any lost block items, avoid meeting unexpected error
    if self._heartbeat_num & 0x3f == 0x3f:   # every 64 * HEARTBEAT_DELAY seconds (about 10 minutes)
      if self._last_incomp:
        incomplete = self._blocks.incomplete_blocks2(self._last_incomp)
        for block in incomplete:
          if block.hash not in self._incomp_blocks:
            self._incomp_blocks[block.hash] = 0
    
    # prepare incomplete block buffer: self._incomp_blocks
    if len(self._incomp_blocks) < self.MAX_INCOMPLETE_BLOCKS: # can handle more
      incomplete = self._blocks.incomplete_blocks(from_block=self._last_incomp,max_count=self.MAX_INCOMPLETE_FETCH)
      if incomplete:
        for block in incomplete:
          hash_ = block.hash
          if hash_ in self._incomp_blocks: continue
          
          msg_block = self._orphans.pop(hash_,None)
          if msg_block:
            try:
              self._txns.add(self,block,msg_block.txns)   # add transactions
            except InvalidBlockException as e:
              self.log('invalid block: %s' % e,level=self.LOG_LEVEL_DEBUG)
              self._incomp_blocks[hash_] = 0  # wait to try again
            except:  # unknown error, just ignore
              traceback.print_exc()
          else:
            self._incomp_blocks[hash_] = 0    # last request time is 0, more than 10 minutes ago
        self._last_incomp = incomplete[-1]
    
    # try request data
    if self._incomp_blocks:
      now = time.time()
      peers = [p for p in self.peers if p.verack]
      random.shuffle(peers)
      
      candidate = None; choosed = None; inflight = 0
      for peer in peers:
        inflight = self._inflight_blocks.get(peer,0)
        if inflight >= self.TOP_INCOMPLETE_INFLIGHT:  # this peer is full, properly connection is abnormal or peer no response
          self._inflight_blocks.pop(peer,None)
          peer.handle_close()
          continue
        elif inflight >= self.MAX_INCOMPLETE_INFLIGHT:
          candidate = peer
          continue
        else:
          choosed = peer
          break
      
      peer = choosed or candidate
      if peer:
        getdata = []
        for hash in self._incomp_blocks:
          if now - self._incomp_blocks[hash] < 600:   # 600 is 10 minutes
            continue
          # else, has not-recently-requested blocks (over 10 minutes ago)
          
          self._incomp_blocks[hash] = now             # will ignored in next loop
          
          getdata.append(protocol.InventoryVector(protocol.OBJECT_TYPE_MSG_BLOCK,hash))
          if len(getdata) > self.BLOCKS_PER_ONE_INFLIGHT:  # one request of peer can not carry too much
            break
        
        if getdata:
          self._inflight_blocks[peer] = self._inflight_blocks.get(peer,0) + len(getdata)       # how many inflight request
          peer.send_message(protocol.GetData(self._link_no,getdata))  # request data
    
    else:  # no block need sync
      if self._unspent._last_valid.height < self.chain_height:
        curr_block = self._unspent.catch_up_check(self)
        self._mine_pool.updateState(self,'new_txns',self.chain_height,curr_block.height)
      else:
        if self._mine_pool.state == 'sync':  # avoid hang off
          self._mine_pool.updateState(self,'new_txns',self.chain_height,self.chain_height)
  
  def block_get_(self, txn):
    return self._blocks._get(txn._blockid)
  
  def block_get2(self, hi):
    return self._blocks._get2(hi)
  
  def block_get3(self, bk_hash):
    return self._blocks.get(bk_hash)
  
  def txns_get(self, txn_hash, only_main=True):
    return self._txns.get(txn_hash,only_main=only_main)
  
  def list_unspent(self, coin_hash, limit=0, _min=0, _max=0, _from=0, _to=0):
    return self._unspent._list_unspent(coin_hash,limit,self._txns,_min,_max,_from,_to)
  
  def list_unspent2(self, uocks, only_mature=True):
    return self._unspent._list_unspent2(uocks,self._txns if only_mature else None)
  
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
      utxos = self._unspent._list_unspent(coin_hash,limit,self._txns,min_utxo,max_utxo,from_uocks[i])
      if sort_flag != 0:  # sort by UTXO's value, low value will count first when it is sorted
        utxos.sort(key = lambda u: u.value)
      
      i += 1
      if i == 1:  # first item of pay_from
        pub_addr = addr
        pub_hash = addr2[1:]  # pub_hash must be assigned since len(pay_from) >= 1
        expected_coin = pub_hash[34:]
        utxos0 = utxos
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
      other_ins.append([protocol.TxnIn(protocol.OutPoint(u.txn_hash,bc_keys.get_uock_index(u.uock)),_EMPTY_SIG,0xffffffff) for u in bIn])
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
      # expected = total + (fee_times + return_num) * self._unspent.TX_MIN_FEE - other_pay
      expected = total + fee_times * self._unspent.TX_MIN_FEE - other_pay  # modified at 2019.01.03
      
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
      tx_ins  = [protocol.TxnIn(protocol.OutPoint(u.txn_hash,bc_keys.get_uock_index(u.uock)),_EMPTY_SIG,0xffffffff) for u in bIn]
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
    
    txn2 = self._txns.get(txn.hash)
    if txn2:
      block = self._blocks._get(txn2._blockid)
      if block and block.mainchain:
        return 'transaction has accepted, confirm = %i' % (self.chain_height - block.height,)
    
    return 'transaction not accept yet'

def startServer(node, db_backup=None):
  ts = Thread(target=node.serve_forever,args=(db_backup,))
  ts.setDaemon(True)
  ts.start()
  time.sleep(0.1)

#-----------

class ZipBackup(object):   # keep same interface with DbBackup
  def __init__(self, data_dir, s3bucket='', s3path=''):
    self._state = 'init'   # 'local_sync' 'cloud_sync' 'exited'
    self._node = None
    self._chain_no = 0
    
    self._data_dir = data_dir
    self._s3bucket = s3bucket
    self._s3path   = s3path
    
    self._last_height = 0
    self._last_valid_hi = 0
    
    self.readonly = True
    self.ec2_reboot = None
  
  @property
  def _active(self):
    if self._state == 'exited': return False
    
    if self._node:
      return not self._node._exited
    else: return True
  
  @property
  def chain_no(self):
    return self._chain_no
  
  def exit(self):
    self._state = 'exited'
  
  def state(self):
    return '%s: last=%i, valid=%i' % (self._state,self._last_height,self._last_valid_hi)
  
  def s3_upload_db(self, local_dir, s3alias='sqlite'):
    if self._s3bucket and self._s3path:
      return bc_database._s3_upload_db(local_dir,self._s3bucket,os.path.join(self._s3path,s3alias))
    else: return 0
  
  def s3_download_db(self, local_dir, s3alias='sqlite'):
    if self._s3bucket and self._s3path:
      return bc_database._s3_download_db(local_dir,self._s3bucket,os.path.join(self._s3path,s3alias))
    else: return 0
  
  def init_db(self, node):
    self._node = node
    self._chain_no = node._chain_no
    
    self._state = 'local_sync'
    print('prepare DB from zip starting...')
    
    # scan local files: zip/00000000-504.zip ...
    zipList = []  # [(0,504),(504,504),(1008,504) ...]
    if os.path.isdir(self._data_dir):  # _data_dir maybe not created yet
      files = {}
      b = os.listdir(self._data_dir)
      for item in b:
        if item[0] == '.': continue
        if item[-4:] == '.zip':
          try:
            files[int(item[:8])] = int(item[9:-4])
          except: pass
      
      iFrom = 0
      while True:
        count = files.get(iFrom,0)
        if not count: break
        zipList.append((iFrom,count))
        iFrom += count
    
    # try download from S3: _s3path/zip/xxxx-xxx.zip
    bc_database.ensure_dir_(self._data_dir)
    from_hi = (zipList[-1][0] + zipList[-1][1]) if zipList else 0
    old_zips = len(zipList)
    print('local exists %i zip file(s), the last height is %i' % (old_zips,from_hi))
    if self._s3bucket and self._s3path:
      bc_database.down_zipped_file(zipList,self._s3bucket,self._s3path,self._data_dir,from_hi)
      print('newly download %i zip file(s)' % (len(zipList)-old_zips,))
    
    if not zipList:
      print('warning: can not find any zip file')
      self._state = 'cloud_sync'   # next will finish by calling db_backup.exit()
      return
    
    # load blocks from local zip files
    prev_hash = self._node._unspent._last_valid.hash
    hi2 = self._node._unspent._last_valid.height + 1
    print('from block height:', hi2)
    
    try:
      for (hi_,num_) in zipList:
        if hi2 < hi_: break
        if hi2 >= hi_ + num_: continue
        
        meetErr, prev_hash = bc_database.load_zipped_block(self._node,prev_hash,hi2,hi_,num_,self._data_dir,self.chain_no)
        
        self._last_valid_hi = self._last_height = self._node._unspent._last_valid.height
        hi2 = self._last_height + 1
        print('... end one zip (hi=%i)' % (self._last_height,))
        
        if meetErr: prev_hash = b''
        if not prev_hash: break
        if self._node._exited: break
    except:
      traceback.print_exc()
    
    self._state = 'cloud_sync'
