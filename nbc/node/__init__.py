
import sys
from six import PY3

try:
  import miniupnpc   # setup by: pip install miniupnpc==2.0.2
except:              # try using builtin miniupnpc
  if PY3:
    if sys.platform == 'darwin':
      from .darwin3 import miniupnpc
    elif sys.platform == 'win32':
      from .win32 import miniupnpc
    else: raise ImportError('failed to import miniupnpc')
  else:
    if sys.platform == 'darwin':
      from .darwin2 import miniupnpc
    elif sys.platform == 'win32':
      from .win32 import miniupnpc
    else: raise ImportError('failed to import miniupnpc')

class AddrInUseErr(Exception): pass
class StopNode(Exception): pass
class InvalidNatErr(Exception): pass

_upnp = None

def init_upnp():
  global _upnp
  if _upnp: return _upnp
  
  u = miniupnpc.UPnP()
  u.discoverdelay = 200
  try:
    print('discovering... delay = %ums' % u.discoverdelay)
    devices = u.discover()
    print('%u device(s) detected' % devices)
    _upnp = u
  except Exception as e:
    print('!! meet Exception: %s' % e)
  return _upnp

def list_redirection():
  if not _upnp: return
  
  i = 0
  while True:
    p = _upnp.getgenericportmapping(i)
    if p == None:
      break
    print(i,p)
    i = i + 1

def add_portmap(port, proto, label=''):
  u = init_upnp()
  try:
    u.selectigd()  # select Internet Gateway Device
    extAddr = u.externalipaddress()
    print('local ip address: %s' % u.lanaddr)
    print('external ip address: %s' % extAddr)
    print('status:%s, type:%s' % (u.statusinfo(), u.connectiontype()))
    
    # find a free port for the redirection
    eport = port
    r = u.getspecificportmapping(eport,proto)
    while r != None and eport < 65536:
      eport = eport + 1
      r = u.getspecificportmapping(eport,proto)
    b = u.addportmapping(eport,proto,u.lanaddr,port,label,'')
    if b:
      print( 'success! redirect %s(%s:%u) => %s(%s:%u)' %
             (proto, extAddr, eport, proto, u.lanaddr, port) )
      return (u,extAddr,eport,u.lanaddr)
    else:
      print('!! redirect upnp port failed')
  except Exception as e:
    print('!! meet Exception: %s' % e)

def remove_portmap(port, proto):
  if not _upnp: return
  try:
    b = _upnp.deleteportmapping(port,proto)
    if b:
      print('successfully deleted port mapping')
    else:
      print('!! failed to remove port mapping')
  except Exception as e:
    print('!! meet Exception: %s' % e)
