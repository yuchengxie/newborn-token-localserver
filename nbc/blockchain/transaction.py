
import os, random, sqlite3, struct, traceback
from binascii import hexlify

import six

from . import database
from . import keys

from .. import coins
from .. import protocol
from .. import script
from .. import util

from ..blockchain import InvalidBlockException

__all__ = ['Database']

if six.PY3:
  xrange = range
  buffer = bytes  # memoryview

def get_q(txid):  # get the index q from a txid
  return struct.unpack('>I',txid[:4])[0]

KEY_MIN_BLOCKHI  = 2

_MINIMUM_N = 4
_N = _MINIMUM_N
_MIN_BLOCKHI = None          # will init as { n:min_block_height }
_CHECK_EVERY_TX = 0x40000    # 0x40000 == 262144

def init_min_blockhi(data_dir, coin_name, db_name):
  global _MIN_BLOCKHI, _N
  _MIN_BLOCKHI = {_MINIMUM_N: 0}
  
  _N = _MINIMUM_N
  n = _N * 2
  
  existAny = False
  while True:
    s = database.make_db_file(data_dir,coin_name,db_name,'-%03d-%03d' % (n,0))
    if not os.path.isfile(s): break
    
    row = None
    try:
      conn = sqlite3.connect(s,timeout=30)
      existAny = True
      
      try:
        cursor = conn.cursor()
        cursor.execute('select value, value_bin from metadata where key = ?',(KEY_MIN_BLOCKHI,))
        row = cursor.fetchone()
      except: pass
      conn.close()
    except: pass
    
    if row:
      _MIN_BLOCKHI[n] = row[0]
      _N = n
      n *= 2
    else:
      print('load KEY_MIN_BLOCKHI failed!')
      _MIN_BLOCKHI = None
      break
  
  return existAny

def get_matched_n(height):
  n = _MINIMUM_N
  while True:
    newN = n * 2
    if newN in _MIN_BLOCKHI:
      if height < _MIN_BLOCKHI[newN]:
        break     # has found n
      else: n = newN
    else: break
  return n

class Transaction(object):
  def __init__(self, database, row, _txn=None):
    keys = [n for (n,t,i) in database.Columns]
    
    self._database = database
    self._txn = _txn
    self._data = dict(zip(keys,row))
    self._po_cache = dict()  # cache for previous output's transactions
  
  version = property(lambda s: s.txn.version)
  inputs = property(lambda s: s.txn.tx_in)
  outputs = property(lambda s: s.txn.tx_out)
  lock_time = property(lambda s: s.txn.lock_time)
  sig_raw = property(lambda s: s.txn.sig_raw)
  
  hash = property(lambda s: s.txn.hash)  # auto cacuated by protocol.Txn
  index = property(lambda s: keys.get_txck_index(s._txck))
  
  def __getstate__(self):         # save state
    return (self._po_cache, dict(txn=str(self._data['txn']),txck=self._data['txck']))
  
  def __setstate__(self, state):  # restore state
    self._database = None
    (self._po_cache, self._data) = state
    self._txn = None
  
  def cache_prev_outputs(self):
    for i in xrange(0, len(self.inputs)):
      self.previous_txn(i)
  
  def previous_txn(self, index):
    if self.index == 0:  # coinbase transaction, all tx_in of coinbase not defined
      return None
    
    # look up the previous output's transaction and cache it
    if index not in self._po_cache:
      po_hash = self.inputs[index].prev_output.hash
      previous_txn = self._database.get(po_hash)  # height is unknown
      self._po_cache[index] = previous_txn        # previous_txn maybe None
      # if not previous_txn: print('fatal error',hexlify(po_hash))
    else: previous_txn = self._po_cache[index]
    
    return previous_txn  # result maybe None
  
  def previous_output(self, index): # output of inputs[index]
    prev_txn = self.previous_txn(index)  # find by po.hash
    if prev_txn is None: return None
    
    po = self.inputs[index].prev_output
    return prev_txn.outputs[po.index]
  
  def __str__(self):
    return "<Transaction hash=0x%s>" % hexlify(self.hash).decode('latin-1')
  
  # transaction composite key and database block id; internal use
  _txck = property(lambda s: s._data['txck'])
  _blockid = property(lambda s: keys.get_txck_blockid(s._data['txck']))
  _mainchain = property(lambda s: s._data['mainchain'])
  
  def _previous_uock(self, index):  # get uock of inputs[index]
    prev_txn = self.previous_txn(index)
    if prev_txn is None: return None
    return keys.get_uock(prev_txn._txck,self.inputs[index].prev_output.index)
  
  @property
  def txn(self):  # raw transaction
    if self._txn is None:
      (_, self._txn) = protocol.Txn.parse(self.txn_bin)  # _ is length
    return self._txn
  
  txn_bin = property(lambda s: bytes(s._data['txn']))    # get binary transaction

db_backup = database.db_backup

class Database(database.Database):
  TARGET_SIZE = (1 << 30) * 7 // 4     # 1.75GB
  
  MIN_BYTES_PER_TXN = 100
  MAX_BYTES_PER_TXN = 65536     # every txn can not large than 64 K
  
  Columns = [
    ('txck', 'integer primary key', False),
    ('txid_hint', 'integer', True),
    ('mainchain', 'boolean not null', False),
    ('txn', 'blob', False),
  ]
  Name = 'txns'
  
  def __init__(self, data_dir=None, coin=coins.Newbitcoin):
    database.Database.__init__(self,data_dir,coin)
    
    self._max_save_hi = 0
    self._add_count = 0
    self._connections = dict()  # {(n,i%n):connection}
    
    if not _MIN_BLOCKHI:
      init_min_blockhi(data_dir,coin.name,self.Name) # set _N, _MIN_BLOCKHI
    
    # load all connections, if inexistent create the entire level
    n = _N
    while n >= _MINIMUM_N:
      self.get_connection(n,0,True,_MIN_BLOCKHI[n])
      n //= 2
    
    # try add genesis transaction
    txck = keys.get_txck(2,0)     # (_blockid=2,index=0)
    genesis_txn = self._get(txck,0)
    if not genesis_txn:     # height=0, genesis txns not added yet, only one transaction
      txn = self.coin.genesis_txn
      if txn:
        conn = self.get_connection(_N,get_q(txn.hash))
        cursor = conn.cursor()
        row = (txck,keys.get_hint(txn.hash),1,buffer(txn.binary()))
        cursor.execute(self.sql_insert,row)
        db_backup.add_task(('+txns',txn.hash,0,txck,1))
        db_backup.add_task(('[txns]',0,None))
        conn.commit()
  
  def get_suffix(self, n, q):
    return '-%03d-%03d' % (n, q % n)
  
  def get_connection(self, n, q, allow_create=False, min_height=None):
    loc = (n, q%n)
    if loc not in self._connections:
      tryCreate = False
      locs = [(n,i) for i in xrange(0,n)]
      if not os.path.isfile(self.get_filename(self.get_suffix(n,0))):
        if not allow_create: return None
        tryCreate = True
        locs.reverse() # if not exist, create the files backward, ensure the last one (n,0) can be created
      
      if tryCreate and min_height is not None:
        sql = 'insert or replace into metadata (key, value, value_bin) values (?, ?, ?)'
      else: sql = ''
      
      for l in locs:
        suffix = self.get_suffix(l[0],l[1])
        conn = database.Database.get_connection(self,suffix) # will create if inexistent
        self._connections[l] = conn
        
        if sql:  # set metadata.KEY_MIN_BLOCKHI when it just be created
          cursor = conn.cursor()
          cursor.execute(sql,(KEY_MIN_BLOCKHI,min_height,None))
          conn.commit()
    
    return self._connections[loc]
  
  def adjust_mainchain(self, txck_from, mainchain, bak_task=[]):
    bak_from = len(bak_task)
    try:
      sql = 'update txns set mainchain = ? where txck >= ? and txck <= ?'
      for conn in self._connections.values():
        cursor = conn.cursor()
        cursor.execute(sql,(mainchain,txck_from,txck_from | 0xfffff))
        bak_task.append(('@txns1',txck_from,{'mainchain_':mainchain}))
        conn.commit()
    except:  # avoid throw error, ignore exception
      del bak_task[bak_from:]  # rollback for db backup
      traceback.print_exc()
  
  def rmv_reject_tx(self, rmv_txck, bak_task=[]):
    bak_from = len(bak_task)
    try:
      sql = 'delete from txns where txck >= ? and txck <= ?'
      for conn in self._connections.values():
        cursor = conn.cursor()
        for txck in rmv_txck:
          cursor.execute(sql,(txck,txck | 0xfffff))
          bak_task.append(('-txns1',txck))
        conn.commit()
    except:  # avoid throw error
      del bak_task[bak_from:]
      traceback.print_exc()
  
  def check_db_size(self, block):
    global _N
    self._add_count += len(block.txns)
    if not block.mainchain: return
    
    if self._add_count >= _CHECK_EVERY_TX and block.height == self._max_save_hi: # all txns before this height has saved
      self._add_count = 0
      
      suffix = self.get_suffix(_N,random.randint(0,_N-1)) # get_q(txid) would be average saving
      filename = self.get_filename(suffix)
      if os.path.getsize(filename) > self.TARGET_SIZE and _MIN_BLOCKHI: # if any is full, increase size
        min_height = self._max_save_hi + 1
        nextN = _N * 2
        for i in range(nextN):
          self.get_connection(nextN,i,True,min_height)
        
        # update _N and _MIN_BLOCKHI
        _N = nextN
        _MIN_BLOCKHI[nextN] = min_height
  
  def add(self, node, block, transactions):
    if block.txn_ready: return block  # ignore
    
    # check txns length
    tx_len = len(transactions)
    if tx_len == 0:
      raise InvalidBlockException('no transaction')
    
    # check merkle_root
    block._check_merkle_root(util.get_merkle_root(transactions))
    
    # save each transaction
    bk_hi = block.height
    mainchain = block.mainchain
    n = get_matched_n(bk_hi)
    connections = dict()
    block_txns = []
    for (idx,txn) in enumerate(transactions):
      txid = txn.hash
      
      # check txn fee
      txn_bin = txn.binary()
      txn_len = len(txn_bin)
      if idx > 0:
        if not node._verify_txn(txn,txn_len):  # check binary size
          node._rmv_mempool(txn.hash)
          raise InvalidBlockException('invalid txn (%i), hash = %s' % (idx,hexlify(txid)))
      else:
        if txn_len > self.MAX_BYTES_PER_TXN or len(txn.tx_out) != 2:
          raise InvalidBlockException('coinbase out of range, hash = %s' % (hexlify(txid),))
      
      # get connection database
      q = get_q(txid)
      conn = self.get_connection(n,q)
      connections[(n,q%n)] = conn
      
      # save one transaction
      cursor = conn.cursor()
      txck = keys.get_txck(block._blockid,idx)   # get transation composite key
      row = (txck,keys.get_hint(txid),mainchain,buffer(txn_bin))
      try:
        cursor.execute(self.sql_insert,row)
      except sqlite3.IntegrityError as e:
        print('warning: txns.txck duplicated (txck=%i)' % txck)  # no matter when duplicates
        try:
          cursor.execute('update txns set mainchain = ? where txck = ?',(mainchain,txck))
          db_backup.add_task(('@txns',txck,{'mainchain_':mainchain}))
        except:
          traceback.print_exc()
      
      # wrap up the transaction for the returned block
      block_txns.append(Transaction(self,row,txn))
    
    # commit the transactions, all txns of one block should be in same n
    for conn in connections.values():
      conn.commit()
    
    # update the block.txns with the transactions
    block._update_txns(tuple(block_txns))
    
    if bk_hi > self._max_save_hi: self._max_save_hi = bk_hi
    
    # if there are too many task waiting backup, just slow down block's process
    db_backup.try_wait_task()
    
    return block
  
  def _get(self, txck, height):
    blockid = keys.get_txck_blockid(txck)
    n = get_matched_n(height)
    
    for ((n2,_),conn) in self._connections.items():
      if n2 != n: continue
      
      cursor = conn.cursor()
      cursor.execute(self.sql_select + ' where txck = ?', (txck,))
      row = cursor.fetchone()
      if row:
        return Transaction(self,row)
    
    return None
  
  def _get_transactions(self, blockid, height, only_mainchain=False):  # smart search all transactions for a block, internal use
    lo = keys.get_txck(blockid,0)    # get transation composite key, range >= lo
    hi = keys.get_txck(blockid+1,0)  # range < hi
    n = get_matched_n(height)
    
    sql = self.sql_select + ' where txck >= ? and txck < ?'
    if only_mainchain: sql += ' and mainchain = 1'
    
    # find all transactions across all databases within this range
    txns = []
    for ((n2,_),conn) in self._connections.items():
      if n2 != n: continue
      cursor = conn.cursor()
      cursor.execute(sql,(lo, hi))
      txns.extend((r[0],r) for r in cursor.fetchall())
    txns.sort()  # sort by txck
    
    # wrap it up in a helpful wrapper
    return [Transaction(self,row) for (txck,row) in txns]
  
  def get(self, txid, default=None, only_main=True): # get mainchain transaction by txid, txid is hash value
    txid_hint = keys.get_hint(txid)  # has index by hint for faster searching
    
    # search each level (n, n // 2, n // 4, etc)
    n = _N
    q = get_q(txid)
    sql = self.sql_select + ' where txid_hint = ?'
    if only_main: sql += ' and mainchain = 1'
    
    while n >= _MINIMUM_N:
      connection = self.get_connection(n,q)
      cursor = connection.cursor()
      cursor.execute(sql,(txid_hint,))
      for row in cursor.fetchall():
        txn = Transaction(self,row)
        if txn.hash == txid:         # get extractly one
          return txn
      n //= 2
    
    return default
