
import struct

from .. import util

def get_txck(blockid, txn_index):
  'Generate a composite key for transactions.'
  
  if (blockid & 0x7fffff) != blockid:       # 24 bits
    raise ValueError('blockid is out of range')
  if (txn_index & 0xfffff) != txn_index:    # 20 bits
    raise ValueError('index is out of range')
  return (blockid << 20) | txn_index

def get_txck_blockid(txck):
  'Get the blockid from a transaction composite key.'
  
  return txck >> 20

def get_txck_index(txck):
  'Get the index from a transaction composite key.'
  
  return txck & 0xfffff

def get_hint(data):
  'Generate a 6-byte hint.'
  
  return struct.unpack('>Q', data[:8])[0] & 0x7fffffffffff

def get_uock(txck, output_index):
  'Generate a composite key for unspend outputs.'
  
  if (txck & 0x7ffffffffff) != txck:   # 44 bits = 32 + 12
    raise ValueError('txck is out of range')
  if (output_index & 0xfffff) != output_index:   # 20 bits
    raise ValueError('output index is out of range')
  
  return (txck << 20) | output_index   # 44 + 20 = 64 bits, avoid using highest bit

def get_uock_txck(uock):
  'Get the transaction composite key from a utxo composite key.'
  
  return uock >> 20

def get_uock_index(uock):
  'Get the output index from a utxo composite key.'
  
  return uock & 0xfffff

def get_address_hint(address):
  'Generate a 6-byte hint for an address with kind field.'
  
  if address is None: return 0
  
  data = util.base58.decode_check(address)
  return get_hint(data[3:35])  # exclude: ver1 vcn2
