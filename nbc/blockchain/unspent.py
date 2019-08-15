
import sqlite3, time, struct, traceback
from binascii import hexlify

import six

from . import database
from . import keys

from .. import coins
from .. import script
from .. import util

from ..blockchain import InvalidBlockException

__all__ = ['Database']

if six.PY3:
  xrange = range
  buffer = bytes  # memoryview

def ORD(ch):   # compatible to python3
  return ch if type(ch) == int else ord(ch)

KEY_LAST_VALID_BLOCK = 2

DOUBLE_SPENT_GAP     = 16

_PAY2MINER = b'\x76\xb8\xb9\x88\xac'

_0 = b'\x00' * 32

def get_block_subsidy(height):
  halvings = height // 200000
  if halvings >= 64: return 1        # when height >= 12800000, at least 1 subsidy
  
  # Subsidy is cut in half every 200,000 blocks which will occur approximately every 700 days
  subsidy = 5000000000               # 50 * 100000000, 1 coin = 10 ** 8
  return (subsidy >> halvings) or 1  # at least return 1

def get_txn_basefee(txn, min_fee):   # txn is instance of Transaction
  (n, mod) = divmod(len(txn.txn_bin),1024)
  if mod: n += 1
  
  # count RETURN number, 0x6a is OP_RETURN
  # return_num = sum((1 if (item.value == 0 and item.pk_script[0:1] == b'\x6a') else 0) for item in txn.outputs)
  # return (n + return_num) * min_fee
  
  return n * min_fee  # modified at 2019.01.03

# class InvalidTransException(Exception): pass

class Unspent(object):
  def __init__(self, database, row):
    keys = [n for (n,t,i) in database.Columns]
    self.__database = database
    self.__data = dict(zip(keys,row))
    self._txn_cache = None
    self._txn_value = 0
  
  uock = property(lambda s: s.__data['uock'])
  height = property(lambda s: s.__data['height'])
  address = property(lambda s: bytes(s.__data['address']))    # hash32 + cointype
  address_hint = property(lambda s: s.__data['address_hint'])
  vcn = property(lambda s: s.__data['vcn'])
  coin_type = property(lambda s: bytes(s.__data['coin_type']))
  
  value = property(lambda s: s._txn_value)   # will be correct after call cache_txn()
  
  def cache_txn(self, txns):
    txck = keys.get_uock_txck(self.uock)
    self._txn_cache = txn = txns._get(txck,self.height)
    if txn:
      index = keys.get_uock_index(self.uock)
      self._txn_value = txn.outputs[index].value
  
  def get_txn(self):
    if self._txn_cache:
      return self._txn_cache.txn
    else: return None
  
  @property
  def pk_script(self):
    if self._txn_cache:
      index = keys.get_uock_index(self.uock)
      return self._txn_cache.outputs[index].pk_script
    else: return None
  
  @property
  def txn_hash(self):
    if self._txn_cache:
      return self._txn_cache.hash
    else: return _0   # not cache transaction yet

db_backup = database.db_backup

class Database(database.Database):
  Columns = [
    ('uock', 'integer primary key', False),  # blockId(24)+inputIndex(20)+outputIndex(20)
    ('height', 'integer', False),            # owner block's height
    ('address', 'blob not null', False),     # public_key_hash + coin type
    ('address_hint', 'integer', True),       # only for quick index, not unique
    ('vcn', 'integer', False),               # vcn of owner account
    ('coin_type', 'blob not null', False),   # for easy management
  ]
  Name = 'unspent'
  
  TX_MIN_FEE = 100                  # 0.00000100 NBC
  TX_TRANSFER_MAX = 1000000000000   # 10000 NBC
  
  def __init__(self, data_dir, blocks, coin=coins.Newbitcoin):
    self.sql_delete = 'delete from ' + self.Name + ' where uock = ?'
    
    database.Database.__init__(self,data_dir,coin)
    self._connection = self.get_connection(iso_level=None)
    self._last_valid = blocks._get(self.get_metadata(self._connection.cursor(),KEY_LAST_VALID_BLOCK))
  
  def populate_database(self, cursor):
    txn = self.coin.genesis_txn
    if txn:
      txck = keys.get_txck(2,0)   # genesis block id is 2
      for (idx,item) in enumerate(txn.tx_out):
        uock = keys.get_uock(txck,idx)
        addr = self.coin.genesis_miner  # public key hash
        coin_type = self.coin.mining_coin_type
        addr_hint = keys.get_hint(addr)
        row = (uock,0,buffer(addr+coin_type),addr_hint,0,buffer(coin_type))  # vcn of genesis coinbase fixed to 0
        cursor.execute(self.sql_insert,row)
        db_backup.add_task(('+utxos',item.value,0,row))
      # self.get_connection().commit()  # warning: can not commit because it still in populating
    
    self.set_metadata(cursor,KEY_LAST_VALID_BLOCK,2)
  
  def _prev_outvalue(self, _txns, po):
    prev_txn = _txns.get(po.hash)
    if not prev_txn:
      raise InvalidBlockException('no previous txn')
    
    try:
      return prev_txn.outputs[po.index].value
    except:    # unknown fatal error
      return 0
  
  def check_txn_cointype(self, txn):
    curr_coin = None
    for i2 in xrange(len(txn.inputs)):
      coin_type_ = script.get_script_cointype(txn.previous_output(i2).pk_script,self.coin)
      if not coin_type_:
        curr_coin = None    # meet error
        break
      
      if curr_coin is None:
        curr_coin = coin_type_
      else:
        if coin_type_ != curr_coin:
          curr_coin = None  # meet error
          break
    
    if curr_coin:
      for item in txn.outputs:
        if item.value == 0 and item.pk_script[0:1] == b'\x6a':  # is OP_RETURN
          continue
        if curr_coin != script.get_script_cointype(item.pk_script,self.coin):
          curr_coin = None  # error: coin type of output mismatch
          break
    
    return curr_coin  # if curr_coin is None means error
  
  def exist_uock(self, uock):
    cursor = self._connection.cursor()
    cursor.execute(self.sql_select + ' where uock = ?',(uock,))
    return True if cursor.fetchone() else False
  
  def catch_up_check(self, _node, new_block=None):
    _blocks = _node._blocks
    
    first_valid = self._last_valid
    if new_block is None:
      new_block = _blocks.get_nextmain(first_valid._blockid)
      if not new_block: return first_valid
    
    if first_valid.height >= new_block.height:
      return first_valid    # nothing to catch up
    
    # assert(first_valid.mainchain and first_valid.txn_ready)
    if not first_valid.mainchain: return first_valid  # meet unknown error
    
    block = first_valid
    if new_block.mainchain and new_block._previous_blockid == block._blockid:
      next_block = new_block  # in most case, reuse new_block.txns
    else: next_block = _blocks.get_nextmain(block._blockid)
    
    changed = False; meet_invalid = False
    invalid_txn = None; invalid_idx = 65535
    cursor = self._connection.cursor()
    
    error = None
    
    while next_block and next_block.txn_ready:
      curr_lnk = next_block.link_no & 0xffff
      bak_task = []
      txns = next_block.txns    # txns must not empty because txn_ready=True
      
      cursor.execute('begin transaction')   # begin immediate transaction
      
      try:
        # step 1: perform coinbase fee checking
        fees = 0
        txns_addrs = [None]                 # None for coinbase
        for (i,txn) in enumerate(txns):
          txn.cache_prev_outputs()          # prepare txn._po_cache={inputIndex:txn}
          
          nn = len(txn.inputs)
          if nn == 0:
            invalid_txn = txn; invalid_idx = i
            raise InvalidBlockException('no transaction input')
          else:
            txnIn = txn.inputs[0]
            if txnIn.prev_output.hash == _0:    # it is coinbase, len(txn.inputs) == 1, len(txn.outputs) == 2
              if txnIn.sequence == 0xffffffff:  # coinbase signature must large than 5 bytes
                if i != 0 or nn != 1 or txnIn.sig_script[:5] != struct.pack('<BI',4,next_block.height) or len(txnIn.sig_script) > 100:
                  invalid_txn = txn; invalid_idx = i
                  raise InvalidBlockException('invalid coinbase')
                # else, coinbase OK
              else:
                invalid_txn = txn; invalid_idx = i
                raise InvalidBlockException('invalid coinbase')
            
            else:  # not coinbase
              if i == 0:
                invalid_txn = txn; invalid_idx = i
                raise InvalidBlockException('invalid coinbase')
              
              # verify coin type
              if not self.check_txn_cointype(txn):
                invalid_txn = txn; invalid_idx = i
                raise InvalidBlockException('coin type mismatch')
              
              # verify txn fee
              sum_in = sum(self._prev_outvalue(_node._txns,item.prev_output) for item in txn.inputs)
              sum_out = sum(item.value for item in txn.outputs)
              fee = sum_in - sum_out
              if fee < get_txn_basefee(txn,self.TX_MIN_FEE) or fee > self.TX_TRANSFER_MAX:
                invalid_txn = txn; invalid_idx = i
                raise InvalidBlockException('invalid fee')
              
              txio = None
              try:
                txio = script.Script(txn,next_block.height,self.coin)
                if not txio.verify(_blocks):  # verify all input signature, check all UTXO should in mainchain also
                  txio = None
                  print('warning: txn (hi=%i,idx=%i) verification failed' % (next_block.height,i))
              except:
                txio = None  # catch all error since 'script' can be anything
                traceback.print_exc()
              
              if not txio:
                invalid_txn = txn; invalid_idx = i
                raise InvalidBlockException('invalid txns: %i' % i)
              
              txio_addrs = [txio.output_addr(i2,_blocks) for i2 in xrange(0,txio.output_count)]
              txns_addrs.append(txio_addrs) # address should be vcn2+hash32+cointype or None
              
              fees += fee
        
        txck_ = keys.get_txck(next_block._blockid,0)
        for (i,txn) in enumerate(txns):     # rewrite it since we not sure txns exists or not
          bak_task.append(('+txns',txn.hash,next_block.height,txck_+i,1))
        bak_task.append(('[txns]',next_block.height,next_block)) # backup to file db first, and avoid overwriting existed mainchain
        
        # step 2: coinbase checking and save UTXO
        coinbase_txn = txns[0]
        coinbase_out = coinbase_txn.outputs
        if len(coinbase_out) == 2:  # output of coinbase fix to 2 items
          if coinbase_out[0].pk_script != _PAY2MINER or coinbase_out[1].pk_script != _PAY2MINER:
            raise InvalidBlockException('invalid pk_script')
          if coinbase_out[0].value != get_block_subsidy(next_block.height):
            raise InvalidBlockException('invalid subsidy')
          
          if coinbase_out[1].value > fees:
            raise InvalidBlockException('invalid txns fees')
          
          addr = next_block.miner
          addr_hint = keys.get_hint(addr)
          
          for idx in range(2):
            uock = keys.get_uock(coinbase_txn._txck,idx)
            coin_type = self.coin.mining_coin_type if idx == 0 else self.coin.currency_coin_type
            
            try:  # save UTXO even if its value is 0
              row = (uock,next_block.height,buffer(addr+coin_type),addr_hint,curr_lnk,buffer(coin_type)) # vcn of coinbase uses chain_no
              cursor.execute(self.sql_insert,row)
              bak_task.append(('+utxos',coinbase_out[idx].value,next_block.height,row))
            except sqlite3.IntegrityError as e: # duplicates is not a matter
              print('warning: unspent.ucok duplicated (uock=%i)' % (uock,))
        else:
          raise InvalidBlockException('invalid coinbase txns')
        
        # assert(len(txns) == len(txns_addrs))
        for i in xrange(1,len(txns)):
          # step 3: update UTXO: remove each input's previous UTXO
          txn = txns[i]
          for idx in xrange(0,len(txn.inputs)):
            uock = txn._previous_uock(idx)
            if uock is None: continue
            
            cursor.execute(self.sql_delete,(uock,))
            bak_task.append(('-utxos',uock))
            if cursor.rowcount != 1:  # previous uock must exits, else double-spend
              print('!! bad state: block (hi=%i,idx=%i,in=%i) failed to delete utxo (uock=%016x)' % (next_block.height,i,idx,uock))
              if txn._blockid >= _blocks._topmost._blockid - DOUBLE_SPENT_GAP:  # can not spend too recent transaction
                print('meet double spent, rowcount=%i' % (cursor.rowcount,))
                invalid_txn = txn; invalid_idx = i
                raise InvalidBlockException('delete UTXO failed (uock=%016x)' % (uock,))
              # else: # not too recent, we have to accept double-spend
          
          # step 4: update UTXO: add each output's UTXO
          addrs = txns_addrs[i]
          for (idx,addr) in enumerate(addrs):  # addr is vcn2+hash32+cointype
            if (not addr) or len(addr) <= 34: continue  # addr can be None when output value is 0
            
            uock = keys.get_uock(txn._txck,idx)
            addr_ = addr[2:]
            addr_hint = keys.get_hint(addr_)
            addr_vcn = (ORD(addr[1]) << 8) + ORD(addr[0])
            try:
              row = (uock,next_block.height,buffer(addr_),addr_hint,addr_vcn,buffer(addr_[32:]))
              cursor.execute(self.sql_insert,row)
              bak_task.append(('+utxos',txn.outputs[idx].value,-1,row))
            except sqlite3.IntegrityError as e: # duplicates is not a matter
              print('warning: unspent.uock duplicated (uock=%i)' % (uock,))
        
        # step 5: commit to DB
        self._connection.commit()
        
        # step 6: loop next
        block = next_block
        next_block = _blocks.get_nextmain(block._blockid)
        bak_task.append(('@valid',block._blockid,block.height))
      
      except InvalidBlockException as e:
        print('invalid block: %s' % e)
        del bak_task[:]
        try:
          self._connection.rollback()
        except: pass
        
        if invalid_txn:
          _node._rmv_mempool(invalid_txn.hash)  # not includes invalid txn later
          bak_task.append(('-pool',invalid_txn.hash))
        
        meet_invalid = True
        break
      except Exception as e:
        del bak_task[:]
        try:
          self._connection.rollback()
        except: pass
        error = e
        break
      
      for task in bak_task:
        db_backup.add_task(task)
    
    if block != first_valid:  # try set KEY_LAST_VALID_BLOCK before re-raised
      self._last_valid = block
      self.set_metadata(cursor,KEY_LAST_VALID_BLOCK,block._blockid)
      changed = True
    
    if error: raise error     # re-raise
    
    if meet_invalid:
      _blocks.reject_block(block,True)
    else:
      if changed:
        _blocks._txns.check_db_size(block)
    return block
  
  def rollback_block(self, node, block, prev_block, bak_task=[]):
    if self._last_valid.height != block.height: return
    if not block.mainchain: return
    # if block.previous_block._blockid != prev_block._blockid: return
    
    txck_from = keys.get_txck(block._blockid,0)
    txck_to   = keys.get_txck(block._blockid+1,0)
    uock_from = keys.get_uock(txck_from,0)
    uock_to   = keys.get_uock(txck_to,0)
    
    txns = block.txns; txn_num = len(txns)
    
    bak_from = len(bak_task)
    cursor = self._connection.cursor()
    cursor.execute('begin transaction')   # begin immediate transaction
    
    try:
      # step 1: try restore removed UTXO
      for idx in xrange(txn_num-1,0,-1):
        txn = txns[idx]   # idx != 0, not process coinbase
        
        for (idx2,item) in enumerate(txn.inputs):
          prev_txn = txn.previous_txn(idx2)
          if not prev_txn: continue
          _block = node._blocks._get(prev_txn._blockid)
          if not _block: continue
          
          po = item.prev_output
          po_value = prev_txn.outputs[po.index].value
          pk_script = prev_txn.outputs[po.index].pk_script
          addr = script.get_script_address(pk_script,node,_block)
          if not addr: continue
          
          uock_ = keys.get_uock(prev_txn._txck,po.index)
          addr_ = addr[2:]  # addr is: vcn2 + hash32 + cointype
          addr_hint = keys.get_hint(addr_)
          addr_vcn = (ORD(addr[1]) << 8) + ORD(addr[0])
          if keys.get_txck_index(prev_txn._txck) == 0:    # is coinbase
            po_hi = _block.height
          else: po_hi = -1
          
          try:
            row = (uock_,_block.height,buffer(addr_),addr_hint,addr_vcn,buffer(addr_[32:]))
            cursor.execute(self.sql_insert,row)
            bak_task.append(('+utxos',po_value,po_hi,row))
          except sqlite3.IntegrityError as e: # duplicates is not a matter
            print('warning: unspent.uock duplicated (uock=%i)' % (uock_,))
      
      # step 2: delete all UTXOs of this block
      cursor.execute('delete from unspent where uock >= ? and uock < ?',(uock_from,uock_to))
      bak_task.append(('-utxos1',txck_from))
      
      # step 3: update self._last_valid and KEY_LAST_VALID_BLOCK
      if self._last_valid.height > prev_block.height:
        self._last_valid = prev_block
        self.set_metadata(cursor,KEY_LAST_VALID_BLOCK,prev_block._blockid)
        bak_task.append(('@valid',prev_block._blockid,prev_block.height))
      
      # step 4: commit
      self._connection.commit()
    except:
      del bak_task[bak_from:]
      self._connection.rollback()
      traceback.print_exc()
  
  def _list_unspent(self, coin_hash, limit=0, _txns=None, _min=0, _max=0, _from=0, _to=0):  # address is: vcn2 + hash32 + cointype
    if not limit: limit = 512         # default search number of unspent
    hint = keys.get_hint(coin_hash)   # coin_hash is: public_key_hash32 + cointype
    
    direct = ''; lt_gt = '>'
    if _from and _from < 0:  # _from = -1 means decreasing
      direct = ' desc'
      lt_gt = '<'
      _from = 0
    
    cursor = self._connection.cursor()
    
    bRet = []
    while len(bRet) < limit:
      sql = self.sql_select
      if not _from:
        if _to:
          sql += ' where uock <= ?'
          param = (_to,hint,buffer(coin_hash),limit)
        else:
          sql += ' where'
          param = (hint,buffer(coin_hash),limit)
      else:
        if _to:
          sql += ' where uock ' + lt_gt + ' ? and uock <= ? and'
          param = (_from,_to,hint,buffer(coin_hash),limit)
        else:
          sql += ' where uock ' + lt_gt + ' ? and'   # uock > _from or uock < _from
          param = (_from,hint,buffer(coin_hash),limit)
      sql += ' address_hint = ? and address = ? order by uock' + direct + ' limit ?'
      
      cursor.execute(sql,param)
      
      b = [Unspent(self,r) for r in cursor.fetchall()]
      if _txns:    # cache txn for easy sum up u.value
        for u in b: u.cache_txn(_txns)
        if not b:
          _from = 0
        else: _from = b[-1].uock
        
        if _max:
          b = list(filter(lambda u: _min < u.value <= _max,b))
        elif _min:  # _min > 0
          b = list(filter(lambda u: _min < u.value,b))
        # else, _min = 0, no change
        for i in xrange(len(b)-1,-1,-1):
          u = b[i]
          if keys.get_txck_index(keys.get_uock_txck(u.uock)) == 0:  # is coinbase
            if self._last_valid.height - u.height < self.coin.COINBASE_MATURITY:  # not maturity yet
              del b[i]
      else:
        _from = 0    # u.value is unknown, should break loop
      
      bRet.extend(b)
      if not _from: break
    
    return bRet      # sum up: sum(u.value for u in bRet)
  
  def _list_unspent2(self, uocks, _txns=None):
    sql = self.sql_select + ' where uock = ?'
    cursor = self._connection.cursor()
    b = []
    for u in uocks:
      cursor.execute(sql,(u,))
      r = cursor.fetchone()
      if r: b.append(Unspent(self,r))
    
    if _txns:
      for u in b: u.cache_txn(_txns)
      
      for i in xrange(len(b)-1,-1,-1):
        u = b[i]
        if keys.get_txck_index(keys.get_uock_txck(u.uock)) == 0:  # is coinbase
          if self._last_valid.height - u.height < self.coin.COINBASE_MATURITY:  # not maturity yet
            del b[i]
    
    return b
