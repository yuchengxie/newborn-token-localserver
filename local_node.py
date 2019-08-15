import sys, os

from nbc import coins, wallet, consensus
from nbc.node import basenode

curr_coin_  = coins.Newborntoken

log_level_  = basenode.BaseNode.LOG_LEVEL_INFO
local_port_ = 30303
local_datadir_ = './database'
local_zipdir_  = ''

chain_no_ = 0

POET_PUBKEY = b'\x03\xdeC%\xe7HO\x8b\xcb\xcb/\x85M\x93\xc9/\xda\x0bm\xbe\xbc\xe7]X\x06|\xeaP\xfb\xe5[\xfd\t'
consensus.init_tee(wallet.Address(pub_key=POET_PUBKEY,vcn=0,coin_type=b'\x00'))

node = None

def paramInArgv(s, _default=''):
  try:
    i = sys.argv.index(s)
    if i > 0 and (i + 1) < len(sys.argv):
      return sys.argv[i+1]
    else: return _default
  except ValueError:
    pass
  return ''

def scanSysArgv():
  global curr_coin_, local_port_, local_datadir_, local_zipdir_, log_level_, chain_no_
  
  sCoin = paramInArgv('--coin')
  if sCoin:
    coin = coins.get_coin(name=sCoin)
    if not coin: coin = coins.get_coin(symbol=sCoin)
    if coin: curr_coin_ = coin
  
  sPort = paramInArgv('--port')
  if sPort:
    local_port_ = int(sPort)
    if local_port_ < 1000 or local_port_ > 65535:
      local_port_ = 30303
  
  sDataDir = paramInArgv('--db')
  if sDataDir: local_datadir_ = sDataDir
  
  sZipDir = paramInArgv('--zip')
  if sZipDir: local_zipdir_ = sZipDir
  
  if paramInArgv('--debug',True):
    log_level_ = basenode.BaseNode.LOG_LEVEL_DEBUG
  
  sChain = paramInArgv('--chain')
  if sChain: chain_no_ = int(sChain)

def main():
  global node
  
  scanSysArgv()
  
  basenode.NODE_ACCESS_ADDR = ('127.0.0.1',local_port_)
  basenode.SyncNode.MEMORY_POOL_SIZE = 3000  # avoid using too much memory
  node = basenode.SyncNode(data_dir=local_datadir_,address=('0.0.0.0',local_port_),link_no=(chain_no_<<16)|chain_no_,coin=curr_coin_)
  node.log_level = log_level_

  db_backup = basenode.ZipBackup(local_zipdir_) if local_zipdir_ else None
  basenode.startServer(node,db_backup)

if __name__ == '__main__':
  main()

  # start_web_server()

# python -i local_node.py --zip ./database/zip
# node.close();
# 
# usage:
#   python -i local_node.py [--zip ./database/zip] [--debug] [--chain 0] [--coin NBT] [--db ./database] [--port 30303]
#
# some testing:
# -------------
# node.log_level = node.LOG_LEVEL_INFO
# node.log_level = node.LOG_LEVEL_DEBUG
# 
# (node._blocks[-1].height, node._unspent._last_valid.height)
# len(node._incomp_blocks)
# node._inflight_blocks
# [(p._peer_ip,p._peer_port) for p in node.peers]
# 
# node.close()  # for safe exit
# 
