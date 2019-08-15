
import math, struct, time, traceback
from binascii import hexlify

from . import keys
from . import database
from . import transaction
from . import unspent

from .. import coins
from .. import util
from .. import consensus

from ..blockchain import InvalidBlockException

__all__ = ['Database']

import six

if six.PY3:
  xrange = range
  buffer = bytes   # buffer = memoryview
# else, uses buffer to represent blob of database in six.PY2

HAEDER_CACHE_SIZE = 48         # 5 mins * 48 is about 4 hours

_0 = b'\x00' * 32              # 32 bytes buffer
_1 = b'\x47' + (b'\x00' * 71)  # FtVarString, for header.sig_tee

db_backup = database.db_backup

class Block(object):
  def __init__(self, database, row):
    keys = [n for (n,t,i) in database.Columns]
    self.__database = database
    self.__data = dict(zip(keys,row))
  
  coin = property(lambda s: s.__database.coin)
  
  version = property(lambda s: s.__data['version'])
  link_no = property(lambda s: s.__data['link_no'])
  
  hash = property(lambda s: bytes(s.__data['hash']))  # convert buffer to bytes in PY2
  merkle_root = property(lambda s: bytes(s.__data['merkle_root']))  # convert buffer to bytes in PY2
  
  timestamp = property(lambda s: s.__data['timestamp'])
  bits = property(lambda s: s.__data['bits'])
  nonce = property(lambda s: s.__data['nonce'])
  txn_count = property(lambda s: s.__data['txn_count'])
  
  height = property(lambda s: s.__data['height'])
  
  miner = property(lambda s: bytes(s.__data['miner']))     # convert buffer to bytes in PY2
  sig_tee = property(lambda s: bytes(s.__data['sig_tee'])) # convert buffer to bytes in PY2
  miner_hint = property(lambda s: s.__data['miner_hint'])
  
  txn_ready = property(lambda s: s.__data['txn_ready'])
  mainchain = property(lambda s: s.__data['mainchain'])
  
  @property
  def txns(self):  # transactions in a block
    if not self.txn_ready: return ()
    
    if 'txns' not in self.__data:
      txns = self.__database._txns._get_transactions(self._blockid,self.height)
      self.__data['txns'] = txns = tuple(txns)
      # assert(self.txn_count == len(txns))
      return txns
    else: return self.__data['txns']
  
  @property
  def previous_block(self):
    cursor = self.__database._cursor()
    cursor.execute(self.__database.sql_select + ' where blockid = ?', (self._previous_blockid,))
    row = cursor.fetchone()
    if row:
      return Block(self.__database,row)
    return None
  
  @property
  def previous_hash(self):
    prev = self.previous_block
    if prev:
      return prev.hash
    else: return _0
  
  @property
  def next_block(self):
    cursor = self.__database._cursor()
    cursor.execute(self.__database.sql_select + ' where previous_id = ?', (self._blockid, ))
    row = cursor.fetchone()
    if row:
      return Block(self.__database, row)
    return None
  
  def __str__(self):
    return '<Block %s>' % (hexlify(self.hash),)
  
  _blockid = property(lambda s: s.__data['blockid'])
  _previous_blockid = property(lambda s: s.__data['previous_id'])
  
  def _check_merkle_root(self, merkle_root):  # internal use
    if merkle_root != self.merkle_root:
      raise InvalidBlockException('invalid merkle root')
  
  def _update_txns(self, txns): # update database and attache txns, internal use
    txn_ready = 1 if len(txns) else 0
    cursor = self.__database._cursor()
    cursor.execute('update blocks set txn_ready = ? where blockid = ?', (txn_ready,self._blockid))
    db_backup.add_task(('@blocks',self._blockid,{'txn_ready_':txn_ready}))
    self.__database._connection.commit()
    
    self.__data['txns'] = txns
    self.__data['txn_ready'] = txn_ready

class Database(database.Database):
  Columns = [
    ('blockid', 'integer primary key', False),
    ('previous_id', 'integer not null', True),
    
    ('hash', 'blob not null', True),
    ('version', 'integer not null', False),
    ('link_no', 'integer not null', False),
    ('merkle_root', 'blob not null', False),
    ('timestamp', 'integer not null', False),
    ('bits', 'integer not null', False),
    ('nonce', 'integer not null', False),
    ('miner', 'blob not null', False),
    ('sig_tee', 'blob not null', False),
    ('txn_count', 'integer not null', False),
    
    ('height', 'integer not null', True),
    ('txn_ready', 'boolean not null', False),
    ('mainchain', 'boolean not null', False),
    ('miner_hint', 'integer', True),
  ]
  
  Name = 'blocks'
  
  def __init__(self, data_dir=None, coin=coins.Newbitcoin):
    self._linkno_table = {}
    self._blockid = 0
    
    database.Database.__init__(self, data_dir, coin)
    self._connection = self.get_connection(iso_level=None)
    self._txns = transaction.Database(self.data_dir,coin)     # used in Block.txns
    self._unspent = unspent.Database(self.data_dir,self,coin)
    
    self.reset_cache()      # at least have one, max 48, set _cache, _topmost
    self._blockid = self._topmost._blockid
  
  def new_blockid(self):
    self._blockid += 1
    return self._blockid
  
  def populate_database(self, cursor):
    # entry for the block previous to the genesis block
    pregenesis = [ 1, 0, buffer(_0), self.coin.genesis_version, 0, buffer(_0),
      0, 0, 0, buffer(_0), buffer(_1), 0, -1, 1, 1, 0 ]  # blockid = 1
    cursor.execute(self.sql_insert, pregenesis)
    
    # add the genesis block
    genesis = [ 2,                               # 0: blockid=2
      1,                                         # 1: previous_id
      buffer(self.coin.genesis_block_hash),      # 2: hash
      self.coin.genesis_version,                 # 3: version
      0,                                         # 4: link_no, fix to 0 for genesis
      buffer(self.coin.genesis_merkle_root),     # 5: merkle_root
      self.coin.genesis_timestamp,               # 6: timestamp
      self.coin.genesis_bits,                    # 7: bits
      self.coin.genesis_nonce,                   # 8: nonce
      buffer(self.coin.genesis_miner),           # 9: miner
      buffer(self.coin.genesis_signature),       # 10: sig_tee
      1,                                         # 11: txn_count, fixed to 1 for genesis
      0,                                         # 12: height
      1,                                         # 13: txn_ready
      1,                                         # 14: mainchain
      keys.get_hint(self.coin.genesis_miner),    # 15: miner_hint
    ]
    cursor.execute(self.sql_insert, genesis)
    self._blockid = 2
    
    db_backup.add_task(('+blocks',tuple(pregenesis)))
    db_backup.add_task(('+blocks',tuple(genesis)))
  
  def close(self):
    self._connection.close()
  
  def _cursor(self):
    return self._connection.cursor()
  
  def reset_cache(self):
    cursor = self._cursor()
    cursor.execute(self.sql_select + ' where mainchain = 1 order by height desc limit %d' % HAEDER_CACHE_SIZE)
    self._caches = [Block(self,row) for row in cursor.fetchall()]   # at least have one, max HAEDER_CACHE_SIZE items
    self._topmost = self._caches[0]
  
  def gen_linkno_table(self, chain, mask):
    link_from = 0; d = {0:(0,0)}   # {mask:(height,link_no)}
    
    sql = self.sql_select + ' where link_no > ? order by height limit 1'
    cursor = self._cursor()
    while True:
      cursor.execute(sql,(link_from,))
      row = cursor.fetchone()
      if not row: break
      
      bk = Block(self,row)
      link_from = bk.link_no   # bk.link_no should be unsigned value
      d[(link_from >> 16) & 0xffff] = (bk.height,link_from & 0xffff)
    
    # check current mask should be max one, and all `history_mask & chain` should be match
    for (m,v) in d.items():
      if m > mask or (chain & m) != v[1]:
        raise Exception('vcn mismatch to DB: blocks.sqlite')
    
    self._linkno_table = d
  
  def verify_linkno(self, node, mask, linkno, height):
    if mask > node._link_mask or linkno != (node._chain_no & mask):  # (node._chain_no & mask) means history chain no
      return False
    
    info = self._linkno_table.get(mask)
    if not info:
      self._linkno_table[mask] = (height,linkno) # add new mask item when local db grow-up
    return True
  
  def add_header(self, node, header, check_exist=True, block_hash=None, fix_id=None):  # add a block to database, block.txns is None means only head is present
    if not block_hash:
      block_hash = util.sha256d(header.binary()[:84])
    if check_exist and self.get(block_hash,orphans=True): return None     # ignore if already exists
    
    # find the previous block
    previous_block = self.get(header.prev_block,orphans=True)
    if not previous_block:
      raise InvalidBlockException('previous block does not exist')
    height = previous_block.height + 1
    
    top_block = self._topmost
    mainchain = bool(height > top_block.height)
    
    # check timestamp
    cursor = self._cursor()
    tm, now = float(header.timestamp), time.time()
    if tm > now + 7200:  # 7200 is 2 hours
      raise InvalidBlockException('timestamp in furture')
    
    if height > 16:
      lastHi, firstHi = self._caches[0].height, self._caches[-1].height
      minHi = height - 16
      if mainchain and height-1 <= lastHi and minHi >= firstHi:
        b16 = []
        for item in self._caches:
          if item.height >= height: continue
          if item.height >= minHi:
            b16.append(item.timestamp)
          else: break    # avoid scaning too many
      else:
        cursor.execute(self.sql_select + ' where height < ? and height >= ? order by height desc',(height,minHi))
        b16 = [Block(self,row) for row in cursor.fetchall()]
        if mainchain:
          b16 = filter(lambda b: b.mainchain, b16)
          b16 = [b.timestamp for b in b16]
        else:
          d16 = dict((b._blockid,b) for b in b16)
          b16 = []; last_ = previous_block
          while last_:
            b16.append(last_.timestamp)
            last_ = d16.pop(last_._previous_blockid,None)
      
      if not b16 or tm <= (sum(b16) // len(b16)):
        raise InvalidBlockException('invalid timestamp (hi=%i)' % (height,))
    # else, ignore first 17 block's time-checking, since genesis's time not accuracy
    
    # check meet PoW condition
    if not consensus.pow_verify(block_hash,height):
      raise InvalidBlockException('invalid nonce for PoW')
    
    # check bits target
    bits0 = previous_block.bits
    if height % 504 == 0:
      pre_period = self._get2(height-504)
      if pre_period and pre_period.bits == bits0:
        standardTm = 120960 + 12 * bits0   # 504 * (240 + bits0/42.0))
        newBits = int(bits0 * ((tm - pre_period.timestamp) / standardTm))  # tm is float, for PY2 also
        newBits = min(bits0*4,max(bits0//4,newBits))
        newBits = min(10000,max(20,newBits))
        
        if header.bits != newBits: newBits = 0
      else: newBits = 0
      if newBits == 0:
        raise InvalidBlockException('invalid new period bits')
    else:
      if bits0 != header.bits:
        raise InvalidBlockException('invalid bits')
    
    # check TEE signature, not check miner's PoET gap at here
    if not consensus.poet_verify(block_hash,header.bits,header.txn_count,header.miner,header.sig_tee):
      raise InvalidBlockException('invalid PoET signature')
    
    # check link mask and link no
    if not self.verify_linkno(node,(header.link_no >> 16) & 0xffff,header.link_no & 0xffff,height):
      raise InvalidBlockException('invalid link_no')
    
    backup_task = []
    forked_bk = None
    cursor.execute('begin transaction')  # begin immediate transaction
    try:
      if mainchain and not previous_block.mainchain:
        # update all blocks from previous_block to the fork as mainchain
        cur = previous_block
        side_branch = []
        while not cur.mainchain:
          side_branch.insert(0,cur)     # increse order
          cur = cur.previous_block
        
        if height - cur.height >= self.coin.FINAL_CONFIRM_LEN:
          raise InvalidBlockException('DISABLE_ROLLBACK')  # fixed, too old for switching branch
        
        forked_bk = cur
        forked_at = forked_bk.hash
        
        # rollback main branch
        cur = top_block
        main_branch = []
        while cur.hash != forked_at:
          main_branch.append(cur)       # decrement order
          cur = cur.previous_block
        
        branch_max = len(main_branch) - 1
        for (idx,cur) in enumerate(main_branch):
          cursor.execute('update blocks set mainchain = 0 where blockid = ?', (cur._blockid,))
          backup_task.append(('@blocks',cur._blockid,{'mainchain_':0}))
          
          tmp = cur._blockid << 20
          self._txns.adjust_mainchain(tmp,0,backup_task)
          
          prev_bk = main_branch[idx+1] if idx < branch_max else forked_bk
          self._unspent.rollback_block(node,cur,prev_bk,backup_task)  # will adjust _unspent._last_valid
        
        for cur in side_branch:
          cursor.execute('update blocks set mainchain = 1 where blockid = ?', (cur._blockid,))
          
          row_ = ( cur._blockid,    # v[0]
            cur._previous_blockid,  # v[1]
            cur.hash,         # v[2]
            cur.version,      # v[3]
            cur.link_no,      # v[4]
            cur.merkle_root,  # v[5]
            cur.timestamp,    # v[6]
            cur.bits,         # v[7]
            cur.nonce,        # v[8]
            cur.miner,        # v[9]
            cur.sig_tee,      # v[10]
            cur.txn_count,    # v[11]
            cur.height,       # v[12]
            cur.txn_ready,    # v[13]
            1,                # v[14], mainchain
            cur.miner_hint )  # v[15]
          backup_task.append(('+blocks',row_)) # rewrite it since we not sure it exists or not
          
          if cur.txn_ready:
            self._txns.adjust_mainchain(cur._blockid << 20,1,backup_task)
      
      # add the block to the database
      if fix_id is None:
        new_id = self.new_blockid()
      else:
        new_id = fix_id
        if self._blockid < fix_id:
          self._blockid = fix_id
      
      row = ( new_id, previous_block._blockid, buffer(block_hash), header.version,
              header.link_no, buffer(header.merkle_root), header.timestamp, header.bits, 
              header.nonce, buffer(header.miner), buffer(header.sig_tee), header.txn_count,
              height, False, mainchain, keys.get_hint(header.miner) )  # txn_ready = False
      cursor.execute(self.sql_insert,row)
      backup_task.append(('+blocks',row))
      
      self._connection.commit()
    except:
      # del backup_task[:]  # no need remove since it will raise exception
      self._connection.rollback()  # self._blockid not rollback
      traceback.print_exc()
      raise
    
    for task in backup_task:
      db_backup.add_task(task)
    
    newBlock = Block(self,row)
    if mainchain and newBlock.height > top_block.height:
      self._topmost = newBlock
    
    if forked_bk:  # maybe cached some items that newly mined because of isolating
      self.reset_cache()
      node.reset_last_incomp()
    elif mainchain:
      idx = -1; cache_added = False
      for item in self._caches:
        idx += 1
        if item.height <= height:  # self._caches has decending sorted by height
          self._caches.insert(idx,newBlock)
          cache_added = True
          break
        # else, item.height > height, continue
      if not cache_added: self._caches.append(newBlock)
      
      while len(self._caches) > HAEDER_CACHE_SIZE:  # topmost will insert to first
        del self._caches[-1]
    # else, not mainchain, ignore add to cache
    
    # if there are too many task waiting to backup, just slow down block's process
    db_backup.try_wait_task()
    
    return newBlock
  
  def _rmv_branch(self, prev_blockid, block, bak_task=[]): # remove block and all that after
    height = root_hi = block.height
    branch = {}; rmv_txck = []; rmv = []
    
    cursor = self._cursor(); iLoop = 0
    while True:
      iLoop += 1
      cursor.execute(self.sql_select + ' where height >= ? and height < ? order by height',(height,height+512))
      blocks = [Block(self,row) for row in cursor.fetchall()]  # has sorted by height, ascending order
      
      if blocks:
        for bk in blocks:
          matched = False
          if iLoop == 1 and bk.height == root_hi:
            if bk._previous_blockid == prev_blockid:
              matched = True  # for root_hi blocks, only one will be matched
          else:
            if bk._previous_blockid in branch:
              matched = True
          
          if matched:
            branch[bk._blockid] = True
            rmv.append((bk._blockid,))
            bak_task.append(('-blocks',bk._blockid))
            if bk.txn_ready:
              rmv_txck.append(keys.get_txck(bk._blockid,0))
        
        if rmv:
          cursor.executemany('delete from blocks where blockid = ?',iter(rmv))
          # cursor.rowcount should same to len(rmv)
          self._connection.commit()
          del rmv[:]
        
        height += 512
      else: break
    
    self.reset_cache()
    self._txns.rmv_reject_tx(rmv_txck,bak_task)
  
  def reject_block(self, block, shift_next=False):   # txns of this block and later must not save to DB
    if shift_next:
      if not block.mainchain: return  # meet unknown error
      prev_block = block
      block = self.get_nextmain(block._blockid)
      if not block: return            # nothing to reject
    else:
      prev_block = block.previous_block
      if not prev_block or not prev_block.mainchain: return  # meet unknown error
    
    bak_task = []
    try:   # remove block and later items in this chain
      self._rmv_branch(prev_block._blockid,block,bak_task)
      for task in bak_task:
        db_backup.add_task(task)
    except:
      traceback.print_exc()
  
  def _get(self, blockid):  # get a block by blockid, internal use
    cursor = self._cursor()
    cursor.execute(self.sql_select + ' where blockid = ?', (blockid,))
    row = cursor.fetchone()
    return Block(self,row) if row else None
  
  def _get2(self, hi):      # get a block by height in mainchain, interal use
    cursor = self._cursor()
    cursor.execute(self.sql_select + ' where height = ? and mainchain = 1',(hi,))
    row = cursor.fetchone()
    return Block(self,row) if row else None
  
  def get_nextmain(self, blockid, default=None): # get block by hash
    sql = ' where previous_id = ? and mainchain = 1 limit 1'
    cursor = self._cursor()
    cursor.execute(self.sql_select + sql,(blockid,))
    row = cursor.fetchone()
    return Block(self,row) if row else default
  
  def get(self, blockhash, default=None, orphans=False): # get block by hash
    sql = ' where hash = ?'
    if not orphans:
      sql += ' and mainchain = 1'
    
    cursor = self._cursor()
    cursor.execute(self.sql_select + sql, (buffer(blockhash),))
    row = cursor.fetchone()
    return Block(self,row) if row else default
  
  def __getitem__(self, name):
    cursor = self._cursor()
    row = None
    if name < 0:  # negative height, from the top
      cursor.execute(self.sql_select + ' where mainchain = 1 order by height desc limit 1')
      row = cursor.fetchone()  # get highest one
      
      if name != -1:  # need search again
        name += Block(self,row).height + 1
        row = None
    
    if name >= 0 and row is None:
      cursor.execute(self.sql_select + ' where mainchain = 1 and height = ?', (name,))
      row = cursor.fetchone()
    
    if row: return Block(self, row)
    raise IndexError()  # not found
  
  def __len__(self):
    highest = self[-1]
    return highest.height + 1
  
  def locate_blocks(self, locator, count=500, hash_stop=None):
    block = None
    for hash in locator:
      block = self.get(hash)
      if block: break          # find one
    if not block: return []    # nothing found
    
    cursor = self._cursor()
    sql = ' where mainchain = 1 and height > ? order by height limit %d' % count
    cursor.execute(self.sql_select + sql, (block.height,))  # try select count rows
    
    blocks = []
    for row in cursor.fetchall():
      block = Block(self,row)  # not include transations
      blocks.append(block)
      if block.hash == hash_stop: break
    return blocks
  
  def block_locator_hashes(self):  # return a list of hashes of recent block
    # Find the height of the block chain
    hashes = []
    
    # try first 10
    offset = 0
    cursor = self._cursor()
    cursor.execute('select hash, height from blocks where mainchain = 1 order by height desc limit 10')
    rows = cursor.fetchall()
    hashes.extend([bytes(hash) for (hash,offset) in rows])
    offset -= 1
    
    # then step down by twice the previous step
    if offset > 0:  # has select any
      for i in xrange(1,int(math.log(2*offset,2))):
        if offset <= 1: break
        cursor.execute('select hash from blocks where mainchain = 1 and height = ?', (offset,))
        one_row = cursor.fetchone()
        if one_row:
          hashes.append(bytes(one_row[0]))
        offset -= (1 << i)   # 1, 2, 4, 8 ...
    
    # finally the genesis hash
    hashes.append(self.coin.genesis_block_hash)
    return hashes
  
  def incomplete_blocks(self, from_block=None, max_count=1000):
    cursor = self._cursor()
    
    if from_block:
      sql = (' where blockid > %d and' % from_block._blockid)
    else: sql = ' where'
    sql += ' txn_ready = 0 and mainchain = 1 order by blockid asc' # txn_ready == 0 means incomplete block
    
    if max_count:
      sql += (' limit %d' % max_count)
    
    cursor.execute(self.sql_select + sql)
    return [Block(self,r) for r in cursor.fetchall()]
  
  def incomplete_blocks2(self, till_block, max_count=1000):
    sql = self.sql_select + ' where blockid <= ? and txn_ready = 0 and mainchain = 1 order by blockid asc limit ?'
    
    cursor = self._cursor()
    cursor.execute(sql,(till_block._blockid,max_count))
    return [Block(self,r) for r in cursor.fetchall()]
  
  def _rmv_last_header(self):  # internal using only, remove topmost when there is only one incomplete
    cursor = self._cursor()
    cursor.execute(self.sql_select + ' where mainchain = 1 order by height desc limit 2')
    b = [Block(self,r) for r in cursor.fetchall()]
    
    if b:
      topmost = b[0]
      if topmost.txn_ready: return False  # topmost is ready, ignore
      if len(b) == 2 and not b[1].txn_ready: return False  # two or more incomplete, ignore
      
      try:
        cursor.execute('delete from blocks where blockid = ?',(topmost._blockid,))
        db_backup.add_task(('-blocks',topmost._blockid))
        self._connection.commit()
        self.reset_cache()
        return True
      except:
        traceback.print_exc()
    
    return False
