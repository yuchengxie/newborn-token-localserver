
import asyncore
import os
import socket
import sys
import time
import traceback

from .. import util
from .. import protocol

from ..blockchain import DbLocker

BLOCK_SIZE = 8192             # one receive block size

_SECONDS_OF_30M  = 30 * 60    #  30 Minutes * 60
_SECONDS_OF_5M   = 5 * 60     #   5 Minutes * 60
_SECONDS_OF_180M = 180 * 60   # 180 Minutes * 60

class Connection(asyncore.dispatcher):
  'Handles buffering input and output into messages and call command_xxx'
  
  SERVICES = protocol.SERVICE_NODE_NETWORK
  
  def __init__(self, node, address, sock=None):
    self._node = node
    self._send_buffer = b''   # send buffer
    self._recv_buffer = b''   # receive buffer
    self._tx_bytes = 0
    self._rx_bytes = 0
    
    self._last_tx_time = 0
    self._last_ping_time = 0
    self._last_rx_time = 0
    
    # remote node details
    self._address = address
    self._self_addr = None
    self._peer_ip = address[0]    # external ip
    self._peer_port = address[1]  # external port, maybe changes to UDP's port
    self._services = None
    self._start_height = None
    self._user_agent = None
    self._version = None
    self._relay = None
    
    self._banscore = 0
    self._verack = False    # get version acknowledgement or not
    
    if sock:  # sock come from listen-accept
      asyncore.dispatcher.__init__(self,sock=sock,map=node) # map: a dictionary whose items are the channels to watch
      self._incoming = True
    else:     # we using an address to connect to
      asyncore.dispatcher.__init__(self,map=node)
      self._incoming = False
      
      try:
        self.create_socket(socket.AF_INET,socket.SOCK_STREAM)
        if node.nat_info and node.nat_info[0] == 1:   # nat_info is None when it is RAW, 1 for NAT_UPNP
          self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
          if hasattr(socket,'SO_REUSEPORT'):
            self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEPORT,1)
          self.bind((node.nat_info[3],node.port))
        self.connect(address)
      except Exception as e:
        self.handle_close()
        raise e
    
    # bootstrap communication with the node by broadcasting ver
    extAddr_ = node.ext_addr
    now = time.time()
    message = protocol.Version( version = node.coin.protocol_version,
                link_no = node.link_no,
                services = self.SERVICES,
                timestamp = now,
                addr_recv = protocol.NetworkAddress(now,self.SERVICES,address[0],address[1]),
                addr_from = protocol.NetworkAddress(now,self.SERVICES,extAddr_[0],extAddr_[1]), # maybe come from basenode.NODE_ACCESS_ADDR
                nonce = os.urandom(8),
                user_agent = node.user_agent,
                start_height = node.chain_height,
                relay = False )
    self.send_message(message)
  
  # remote node details
  address = property(lambda s: s._address)
  ip = property(lambda s: s._address[0])
  port = property(lambda s: s._address[1])
  
  incoming = property(lambda s: s._incoming)
  
  services = property(lambda s: s._services)
  start_height = property(lambda s: s._start_height)
  user_agent = property(lambda s: s._user_agent)
  version = property(lambda s: s._version)
  relay = property(lambda s: s._relay)
  
  # connection details
  verack = property(lambda s: s._verack)
  rx_bytes = property(lambda s: s._rx_bytes)
  tx_bytes = property(lambda s: s._tx_bytes)
  
  node = property(lambda s: s._node)
  banscore = property(lambda s: s._banscore)
  
  # last time we heard from remote
  timestamp = property(lambda s: (time.time() - s._last_rx_time))
  
  def add_banscore(self, penalty=1):
    self._banscore += penalty
  
  def reduce_banscore(self, penalty=1):
    i = self._banscore - penalty
    self._banscore = 0 if i < 0 else i
  
  def readable(self):
    now = time.time()
    rx_ago = now - self._last_rx_time
    tx_ago = now - self._last_tx_time
    ping_ago = now - self._last_ping_time
    
    # haven't sent anything for 30 minutes, send a ping every 5 minutes
    if self._last_tx_time and tx_ago > _SECONDS_OF_30M and ping_ago > _SECONDS_OF_5M:
      self.send_message(protocol.Ping(os.urandom(8)))
      self._last_ping_time = time.time()
    
    # it's been over 3 hours, just disconnect
    if self._last_rx_time and rx_ago > _SECONDS_OF_180M:
      self.handle_close()
      return False
    return True
  
  def handle_read(self):
    try:
      chunk = self.recv(BLOCK_SIZE)
      if not chunk: return
    except Exception as e:
      traceback.print_exc()
      self.handle_close()
      return
    
    self._recv_buffer += chunk
    self._rx_bytes += len(chunk)
    self.node._rx_bytes += len(chunk)
    self._last_rx_time = time.time()
    
    # process as many messages
    while True:
      length = protocol.Message.first_msg_len(self._recv_buffer)
      if length is None or length > len(self._recv_buffer):
        break  # not enough bytes for next message
      
      # parse the message and handle it
      payload = self._recv_buffer[:length]
      self._recv_buffer = self._recv_buffer[length:]  # remove one message
      
      if length > 0x2100000:    # 0x2100000 is 34.6 M
        self._recv_buffer = b'' # it mostly meet some unexpected error
        self.node.log('exceed max message length',peer=self,level=self.node.LOG_LEVEL_ERROR)
        continue                # ignore process this message
      
      DbLocker.enter()
      try:
        message = protocol.Message.parse(payload,self.node.coin.magic)
        self.node._threadDeep += 1
        try:
          self.node._bytes_ = len(payload)
          self.handle_message(message)
        except:
          traceback.print_exc()
        self.node._threadDeep -= 1
      except protocol.UnknownMsgError as e:
        self._recv_buffer = b''   # avoid chaos bytes
        self.node.invalid_command(self,payload,e)
      except protocol.MsgFormatError as e:
        self._recv_buffer = b''   # avoid chaos bytes
        self.node.invalid_command(self,payload,e)
      except:    # just print error, avoid stopping
        self._recv_buffer = b''   # avoid chaos bytes
        self.node.log(traceback.format_exc(),peer=self,level=self.node.LOG_LEVEL_ERROR)
      DbLocker.leave()
  
  def writable(self):
    return len(self._send_buffer) > 0
  
  def handle_write(self):
    try:
      sent = self.send(self._send_buffer)
      self._tx_bytes += sent
      self.node._tx_bytes += sent
      self._last_tx_time = time.time()
    except Exception as e:
      self.handle_close()
      return
    self._send_buffer = self._send_buffer[sent:]
  
  def handle_error(self):
    t,v,tb = sys.exc_info()
    if t == socket.error:
      self.node.log('--- connection refused: %s' % v,peer=self,level=self.node.LOG_LEVEL_INFO)
    else:
      self.node.log(traceback.format_exc(),peer=self,level=self.node.LOG_LEVEL_ERROR)
    del tb
    self.handle_close()
  
  def handle_close(self):
    try:
      self.close()
    except Exception as e:
      traceback.print_exc()
      pass
    self.node.disconnected(self)
  
  def handle_message(self, msg):
    logLevel = self.node.log_level
    if logLevel <= self.node.LOG_LEVEL_PROTOCOL:
      self.node.log('<<< ' + str(msg), peer=self, level=logLevel)
    elif logLevel <= self.node.LOG_LEVEL_DEBUG:
      self.node.log('<<< ' + msg._debug(), peer=self, level=logLevel)
    
    if msg.command == protocol.Version.command:
      if self.node.link_no != msg.link_no:
        self.node.log('error: mismatch link no',peer=self,level=self.node.LOG_LEVEL_INFO)
        self.handle_close()
        msg = None
      else:
        self._services = msg.services
        self._start_height = msg.start_height
        self._user_agent = msg.user_agent
        self._version = msg.version
        self._relay = msg.relay
        self._self_addr = (msg.addr_recv.address,msg.addr_recv.port)
        self._peer_ip = msg.addr_from.address  # for open or upnp, it is open address, otherwise use UDP external
        self._peer_port = msg.addr_from.port
        
        peerAddr = (self._peer_ip,self._peer_port)
        if peerAddr in self.node._banned:
          if time.time() - self.node._banned[peerAddr] < 3600: # punish peer within one hour
            self.handle_close()
            msg = None
          else:
            self.node._banned.pop(peerAddr,None)  # ignore early banned list
        
        if msg:
          if (not self._incoming) and peerAddr != self._address:
            if self._address in self.node._raw_alias:
              self.node._raw_alias[peerAddr] = True
            else: self.node._peer_alias[self._address] = peerAddr
          
          # try close old peer which has same address
          selfId = id(self)
          peers_ = self.node.peers
          for peer in peers_:
            if selfId != id(peer) and not peer._incoming:      # peer is not self and is connecting-to
              if peerAddr == (peer._peer_ip,peer._peer_port):  # exist same peer
                print('here',[(p._peer_ip,p._peer_port) for p in peers_],self._address,peerAddr,self.node._raw_alias,self.node._peer_alias)
                peer.handle_close()
          
          self.node.is_connected(self)
    
    elif msg.command == protocol.VersionAck.command:
      self._verack = True
    
    if msg:
      method = getattr(self.node,'command_'+msg.name,None)
      if method:
        kwargs = dict((k,getattr(msg,k)) for (k,t) in msg.properties)
        self.node._msg_ = msg  # cache it for method()
        method(self,**kwargs)
      else:
        self.node.log('error: method not defined: command_%s' % msg.name, peer=self, level=self.node.log_level)
  
  def send_message(self, message):
    logLevel = self.node.log_level
    if logLevel <= self.node.LOG_LEVEL_PROTOCOL:
      self.node.log('>>> ' + str(message), peer=self, level=logLevel)
    elif logLevel <= self.node.LOG_LEVEL_DEBUG:
      self.node.log('>>> ' + message._debug(), peer=self, level=logLevel)
    
    self._send_buffer += message.binary(self.node.coin.magic)
  
  def __hash__(self):
    return hash(self.address)
  
  def __str__(self):
    return '<Connection(%s) %s:%d>' % (self._fileno,self.ip,self.port)
