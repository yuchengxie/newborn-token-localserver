
import sys, time, struct
from binascii import hexlify

from nbc import util
from nbc import wallet
from nbc import consensus

from nbc.coins import Newborntoken as coin

def main():
  if len(sys.argv) < 4:
    print('usage: python make_genesis.py account_poet.txt account_admin.txt password')
    return
  
  poet  = wallet.loadFrom(sys.argv[1],sys.argv[3])
  consensus.init_tee(poet)
  
  miner = wallet.loadFrom(sys.argv[2],sys.argv[3])
  pubHash = util.key.publickey_to_hash(miner.publicKey(),0)  # vcn=0
  pubAddr = util.key.publichash_to_address(pubHash,0)  # base58 address, vcn=0, version=b'\x00'
  # coin.genesis_miner = pubHash
  
  timestamp = int(time.time())
  merkle_root = util.get_merkle_root([coin.genesis_txn])
  # coin.genesis_timestamp = timestamp
  # coin.genesis_merkle_root = merkle_root
  
  th = consensus.PowThread()
  th.start()
  
  nonce = 0
  block_hash = b''
  header0 = util.get_block_header2(coin.genesis_version,0,b'\x00'*32,merkle_root,timestamp,coin.genesis_bits) # link_no=0
  idx = th.solvePow(header0)
  while True:
    succ,idx2,nonce2,sHash = th.getResult()
    if succ:  # assert(idx2 == idx)
      nonce = nonce2
      block_hash = sHash
      break
    time.sleep(2)
  # coin.genesis_nonce = nonce
  # coin.genesis_block_hash = block_hash
  
  sig = consensus.poet_sign(block_hash,coin.genesis_bits,1,b'\x00',pubHash)
  # coin.genesis_signature = sig
  
  print('make genesis block header successful:')
  print('  genesis miner: %s\n' % (pubAddr,))
  
  print('  genesis_block_hash  = %s' % (hexlify(block_hash),))
  print('  genesis_merkle_root = %s' % (hexlify(merkle_root),))
  print('  genesis_timestamp   = %s' % (timestamp,))
  print('  genesis_miner       = %s' % (hexlify(pubHash),))
  print('  genesis_nonce       = %s' % (nonce,))
  print('  genesis_signature   = %s' % (hexlify(sig),))

if __name__ == '__main__':
  main()
