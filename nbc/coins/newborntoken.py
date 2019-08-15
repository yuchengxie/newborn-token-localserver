# nbc/wallet: wallet APP for NBT token, for details please visit http://nb-chain.net/
# Copyright (C) 2019 Wayne Chan. Licensed under the Mozilla Public License,
# Please refer to http://mozilla.org/MPL/2.0/

import struct

from .. import util
from .. import protocol

from . import coin

__all__ = ['Newborntoken']

_PAY2MINER = b'\x76\xb8\xb9\x88\xac'  # DUP OP_HASH512 OP_MINERHASH OP_EQUALVERIFY OP_CHECKSIG

import codecs
_decodeHex = codecs.getdecoder("hex_codec")

def decodeHex(s):  # avoid using 'ff'.decode('hex') that not supported in python3
  return _decodeHex(s)[0]

class Newborntoken(coin.Coin):
  FINAL_CONFIRM_LEN = 504
  COINBASE_MATURITY = 16
  
  WEB_SERVER_ADDR = 'http://raw0.nb-chain.net'
  
  name    = "newborntoken"
  symbols = ['NBT']         # all symbols
  symbol  = symbols[0]      # primary symbol
  
  mining_coin_type   = b'\x00'
  currency_coin_type = b'\x00'
  protocol_version   = 0    # protocol ver
  block_version      = 1    # block format ver, less changing than protocol ver
  
  magic = b'\xf9\x6e\x62\x74'
  
  raw_seed = ('raw%.nb-chain.net',20303)    # 'raw%.nb-chain.net' or '52.80.85.68', tcp listen port is 20303
  
  genesis_version = 1
  genesis_block_hash = decodeHex(b'1f4bb08cbc3370746a3de301511ab7395d2b439e497dc604d9062341a90d0000')
  genesis_merkle_root = decodeHex(b'e2fb0b95bc2294d046646592df8ffee4cf6df21a0cef0d95e9c712b45a7eddc0')
  genesis_timestamp = 1546517099
  genesis_bits = 2500
  genesis_miner = decodeHex(b'be599666b155b9a4e87502f55aea4def3917a33f6d11672004a98304060ee8b8')
  genesis_nonce = 47961596
  genesis_signature = decodeHex(b'304402203d0894fbbae2f82657af91852e940ab87c2a000b97a1ed24ddb449caadff72be02202b99ad651aabd82a7822da763ca68cb9e6aaae1e9507af04d47a4526d20994cf00')
  genesis_txn = protocol.Txn( 1,
      [protocol.TxnIn(protocol.OutPoint(b'\x00'*32,0xffffffff),struct.pack('<BI',4,0),0xffffffff)],
      [protocol.TxnOut(1050000000000000,_PAY2MINER),protocol.TxnOut(0,_PAY2MINER)],
      0xffffffff, b'' ) # genesis block only contains one transaction
