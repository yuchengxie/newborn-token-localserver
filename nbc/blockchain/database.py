
import time, re, os, sqlite3, traceback, base64, csv
from threading import Thread, current_thread
from io import BytesIO
from binascii import hexlify, unhexlify

import six

from .. import coins, util, protocol, script
from ..protocol import format as _format

from . import keys
from ..blockchain import InvalidBlockException

if six.PY3:
  buffer = bytes  # memoryview
  xrange = range

def ORD(ch):   # compatible to python3
  return ch if type(ch) == int else ord(ch)

def ensure_dir_(a_dir):
  if not a_dir or os.path.exists(a_dir): return
  
  ensure_dir_(os.path.split(a_dir)[0])
  os.mkdir(a_dir)

KEY_VERSION = 1

Key = None
Attr = None
dynamodb = None
s3 = None

#------------------------------
import zipfile

_re_newline = re.compile(b'\\r\\n|\\n|\\r')

def down_zipped_file(zip_list, s3db_bucket, s3db_path, zip_dir, from_hi):
  global s3
  if not s3:
    import boto3
    s3 = boto3.resource('s3',region_name=os.environ.get('AWS_REGION'))
  
  bkt = s3.Bucket(s3db_bucket)
  
  while True:
    # find next zip file from s3
    nextKey = ''
    try:
      fileSet = bkt.objects.filter(Prefix = s3db_path + ('/zip/%08i-' % (from_hi,)), MaxKeys=1)
      for item in fileSet:  # item is ObjectSummary
        nextKey = item.key
        break
    except: pass
    if not nextKey: break
    
    # download zip file
    try:
      num = 0
      sFile = os.path.split(nextKey)[-1]
      b = re.split(r'\W+',sFile)
      if len(b) == 3 and b[-1] == 'zip':  # ['00000000','504','zip'] for '00000000-504.zip'
        num = int(b[1])
      
      if num:
        sLocal = os.path.join(zip_dir,sFile)
        try:
          with open(sLocal,'wb') as income:
            bkt.download_fileobj(nextKey,income)
        except:      # try download again
          try:
            with open(sLocal,'wb') as income:
              bkt.download_fileobj(nextKey,income)
          except:
            num = 0  # means failed
            traceback.print_exc()
      if not num: return False  # meet error
      
      zip_list.append((from_hi,num))
      from_hi += num
    except:
      traceback.print_exc()
      return False  # meet error
  
  return True

def load_zipped_block(_node, prev_hash, from_hi, hi, num, zip_dir, chain_no):
  _blocks  = _node._blocks
  _unspent = _node._unspent
  _txns    = _node._txns
  
  zipFile = '%08i-%i.zip' % (hi,num)
  z = zipfile.ZipFile(os.path.join(zip_dir,zipFile),'r')
  print('start importing from local zip (%s) ...' % (zipFile,))
  
  meetErr = False
  try:
    for i in range(from_hi,hi+num):
      if (i % 64) == 63: time.sleep(0.05)  # avoid occupy too much CPU, EC2 maybe de-level-ed if long time occupy
      if _node._exited: break              # safe quit heavy task
      
      b = _re_newline.split(z.read('%i-%i.csv' % (chain_no,i)))
      
      if b: del b[0]  # remove header line
      if len(b) < 2:  # at least have: head, one txn
        print('warning: invalid csv format')
        meetErr = True
        break
      
      b2 = b[0].split(b',')  # idx,hash,content
      head = _format.BlockHeader.parse(base64.b64decode(b2[2]))[1] 
      del b[0]
      
      txns = []
      for item in b:
        b2 = item.split(b',')
        if len(b2) == 3:
          txns.append(_format.Txn.parse(base64.b64decode(b2[2]))[1])
      if head.prev_block != prev_hash:
        print('sync block failed at height (%i): hash mismatch' % (i,))
        meetErr = True
        break
      if head.txn_count != len(txns):
        print('sync block failed at height (%i): len(txns) mismatch' % (i,))
        meetErr = True
        break
      
      msg_block = protocol.Block( head.version, head.link_no, head.prev_block,
        head.merkle_root, head.timestamp, head.bits,
        head.nonce, head.miner, head.sig_tee, txns )
      
      curr_block = None
      try:
        curr_block = _blocks.add_header(_node,msg_block.make_header(),False,head.hash) # False means no-check-duplicate
        _txns.add(_node,curr_block,txns) # add transactions
      except InvalidBlockException as e:
        if curr_block:
          _blocks.reject_block(curr_block)
        raise e;
      
      if curr_block:
        if curr_block.mainchain:
          _unspent.catch_up_check(_node,curr_block)
          if _unspent._last_valid.height != i:
            print('fatal error: invalid block (hi=%i)' % (i,))
            meetErr = True
            break
          else:   # success
            prev_hash = curr_block.hash
        else:
          print('fatal error: block (hi=%i) should be in mainchain' % (i,))
          meetErr = True
          break   # meet unexpected error
      else:
        print('fatal error: add block header (hi=%i) failed' % (i,))
        meetErr = True
        break     # meet unexpected error
  except:
    meetErr = True
    traceback.print_exc()
  
  z.close()
  return (meetErr,prev_hash)

def _s3_upload_db(local_dir, s3bucket, db_path):
  global s3
  if not s3:
    import boto3
    s3 = boto3.resource('s3',region_name=os.environ.get('AWS_REGION'))
  
  print('start upload db to s3:',local_dir,s3bucket,db_path)
  upNum = 0
  b = os.listdir(local_dir)
  for item in b:
    if item[0] == '.': continue
    if item.split('.')[-1] != 'sqlite': continue
    
    succ = True; s3path = db_path + '/' + item
    try:
      with open(os.path.join(local_dir,item),'rb') as f:
        s3.upload_fileobj(f,s3bucket,s3path)
    except:
      try:  # try second time
        with open(os.path.join(local_dir,item),'rb') as f:
          s3.upload_fileobj(f,s3bucket,s3path)
      except:
        traceback.print_exc()
        succ = False
    
    if succ:
      upNum += 1
      print('upload success:',s3path)
    else:
      print('upload failed:',s3path)
      return 0
  
  return upNum

def _s3_download_db(local_dir, s3bucket, db_path):
  global s3
  if not s3:
    import boto3
    s3 = boto3.resource('s3',region_name=os.environ.get('AWS_REGION'))
  
  print('start download db from s3:',local_dir,s3bucket,db_path)
  downNum = 0
  bkt = s3.Bucket(s3bucket)
  fileSet = bkt.objects.filter(Prefix=db_path+'/',Delimiter='/')
  for item in fileSet:  # item is ObjectSummary
    nextKey = item.key
    fileName = nextKey.split('/')[-1]
    if not fileName or fileName[-7:] != '.sqlite': continue
    
    succ = True; localFile = os.path.join(local_dir,fileName)
    try:
      with open(localFile,'wb') as data:
        s3.download_fileobj(s3bucket,db_path+'/'+fileName,data)
    except:
      try:   # try second time
        with open(localFile,'wb') as data:
          s3.download_fileobj(s3bucket,db_path+'/'+fileName,data)
      except:
        traceback.print_exc()
        succ = False
    
    if succ:
      downNum += 1
      print('download success:',localFile)
    else:
      print('download failed:',localFile)
      return 0
  
  return downNum

class BlockPacker:
  def __init__(self):
    self._data_dir = ''
    self._chain_no = 0
    self._last_local = None   # (block_start_hi,block_num)
  
  @property
  def chain_no(self):
    return self._chain_no
  
  def scan_local(self):
    if not self._data_dir: return []
    
    files = {}
    b = os.listdir(self._data_dir)
    for item in b:
      if item[0] == '.': continue
      if item[-4:] == '.zip':
        try:
          files[int(item[:8])] = int(item[9:-4])
        except: pass
    
    out = []    # [(0,504),(504,504),(1008,504) ...]
    iFrom = 0
    while True:
      count = files.get(iFrom,0)
      if not count: break
      out.append((iFrom,count))
      iFrom += count
    return out
  
  def pack_one_zip(self, start, num, _blocks):
    if not self._data_dir: raise Exception('zip directory not defined')
    
    sFile = os.path.join(self._data_dir,'%08i-%i.zip' % (start,num))
    zFile = zipfile.ZipFile(sFile,'w',zipfile.ZIP_STORED) # zipfile.ZIP_DEFLATED
    try:
      for hi in range(start,start+num):
        bk = _blocks._get2(hi)
        hash_str = hexlify(bk.hash)
        head_bin = _format.BlockHeader.from_block(bk).binary()
        
        output = b'index,hash,content\r\n'
        output += b'%i,%s,%s\r\n' % (-1,hash_str,base64.b64encode(head_bin))
        for (idx,txn) in enumerate(bk.txns):
          output += b'%i,%s,%s\r\n' % (idx,hexlify(txn.hash),base64.b64encode(txn.txn_bin))
        
        zFile.writestr('%i-%i.csv' % (self.chain_no,hi),output)
      zFile.close()
    
    except:
      traceback.print_exc()
      
      zFile.close()
      try:
        os.remove(sFile)
      except: pass
      sFile = ''     # means failed
    
    return sFile
  
  def init_local(self, data_dir, chain_no):
    self._data_dir = data_dir
    self._chain_no = chain_no
    
    if data_dir:
      ensure_dir_(data_dir)
      
      out = self.scan_local()
      if out:
        self._last_local = out[-1]
      # else, self._last_local = None
      return out
    else: return []
  
  def pack_upload_zip(self, from_hi, num, _blocks, bucket, s3path):  # s3path should not includes '/zip/'
    succ = False
    sFile = self.pack_one_zip(from_hi,num,_blocks)
    
    if sFile:
      succ = True
      try:
        inBytes = open(sFile,'rb').read()
        sFile = s3path + '/zip/' + os.path.split(sFile)[-1]
        
        for i in range(3):
          try:
            s3.Bucket(bucket).put_object(Key=sFile,Body=inBytes)
            break
          except:
            if i == 2:
              succ = False
              traceback.print_exc()
      except:
        succ = False
        traceback.print_exc()
    
    if succ:
      self._last_local = (from_hi,num)
    return succ

block_packer = BlockPacker()

#------------------------------

_0 = b'\x00' * 32

def get_set_expr(keyValue):
  i = 0
  expr = 'SET'; value = {}
  for (k,v) in keyValue:
    if i > 0: expr += ','
    i += 1
    
    ss = ':v' + str(i)
    expr += ' %s = %s' % (k,ss)
    value[ss] = v
  
  return (expr,value)

class DbException(Exception): pass

class DbBackup(Thread):
  MAX_BACKUP_TASK = 2000
  MID_BACKUP_TASK = 1000
  LOW_BACKUP_TASK =  500
  
  MAX_TRY_COUNT = 100
  
  def __init__(self):
    Thread.__init__(self)
    self.daemon = True
    self._active = False
    
    self.s3db_bucket = 'nb-filedb'
    self.s3db_path = 'txn_files'
    
    self.tb_blocks = None
    self.tb_txns   = None
    self.tb_utxos  = None
    self.tb_states = None
    
    self._node = None
    self._coin = None
    self._blocks = None
    self._chain_no = 0
    self._tasks = []
    
    self._state = 'init'       # init local_sync cloud_sync exited
    self._last_backup_hi = 0   # for debug
    
    self._last_height = 0      # last height of block header
    self._last_valid_id = 0
    self._last_valid_hi = -1   # last catched up block height
    
    self.readonly = False
    self.ec2_reboot = None
  
  @property
  def chain_no(self):
    return self._chain_no
  
  def _safe_query(self, tb, **kwargs):
    tryNum = 0
    while True:
      try:
        res = tb.query(**kwargs)
        status = res.get('ResponseMetadata',{}).get('HTTPStatusCode',500)
        tryNum += 1
        
        if status >= 500:
          if tryNum > self.MAX_TRY_COUNT:
            print('fatal error: dynamodb meet max tries')
            return None        # error
        elif status == 200:
          return res.get('Items')
        
        time.sleep(min(tryNum,30))  # hold on and waiting network repaired
      except Exception as e:
        print(kwargs)
        print('dynamodb (%s) error: %s' % (tb, e))
        return None
  
  def _safe_query_count(self, tb, **kwargs):
    kwargs['Select'] = 'COUNT'  # force set 'COUNT'
    
    tryNum = 0
    while True:
      try:
        res = tb.query(**kwargs)
        status = res.get('ResponseMetadata',{}).get('HTTPStatusCode',500)
        tryNum += 1
        
        if status >= 500:
          if tryNum > self.MAX_TRY_COUNT:
            print('fatal error: dynamodb meet max tries')
            return None        # error
        elif status == 200:
          return res.get('Count',0)
        
        time.sleep(min(tryNum,30))  # hold on and waiting network repaired
      except Exception as e:
        print(kwargs)
        print('dynamodb (%s) error: %s' % (tb, e))
        return None
  
  def _safe_put_item(self, tb, **kwargs):
    tryNum = 0
    while True:
      try:
        res = tb.put_item(**kwargs)
        status = res.get('ResponseMetadata',{}).get('HTTPStatusCode',500)
        tryNum += 1
        
        if status >= 500:
          if tryNum > self.MAX_TRY_COUNT:
            print('fatal error: dynamodb meet max tries')
            return False            # error
        elif status == 200:
          return True
        
        time.sleep(min(tryNum,30))  # hold on and waiting network repaired
      except Exception as e:
        print(kwargs)
        print('dynamodb (%s) error: %s' % (tb, e))
        return False
  
  def _safe_update(self, tb, **kwargs):
    tryNum = 0
    while True:
      try:
        res = tb.update_item(**kwargs)
        status = res.get('ResponseMetadata',{}).get('HTTPStatusCode',500)
        tryNum += 1
        
        if status >= 500:
          if tryNum > self.MAX_TRY_COUNT:
            print('fatal error: dynamodb meet max tries')
            return False            # error
        elif status == 200:
          return True
        
        time.sleep(min(tryNum,30))  # hold on and waiting network repaired
      except Exception as e:
        print(kwargs)
        print('dynamodb (%s) error: %s' % (tb, e))
        return False
  
  def _safe_remove(self, tb, **kwargs):
    tryNum = 0
    while True:
      try:
        res = tb.delete_item(**kwargs)
        status = res.get('ResponseMetadata',{}).get('HTTPStatusCode',500)
        tryNum += 1
        
        if status >= 500:
          if tryNum > self.MAX_TRY_COUNT:
            print('fatal error: dynamodb meet max tries')
            return False            # error
        elif status == 200:
          return True
        
        time.sleep(min(tryNum,30))  # hold on and waiting network repaired
      except Exception as e:
        print(kwargs)
        print('dynamodb (%s) error: %s' % (tb, e))
        return False
  
  def get_last_block(self):
    tm = int(time.time())
    m = tm % 31104000   # 24 * 3600 * 360 = 31104000  # about one year
    tm -= m
    
    param = { 'TableName':'blocks', 'IndexName':'lnk_date-lnk_height-index',
      'Limit':1, 'ScanIndexForward':False,
      'KeyConditionExpression': Key('lnk_date_').eq((self.chain_no << 32) | tm),
    }
    res = self._safe_query(self.tb_blocks,**param)
    
    if not res and type(res) == list:  # not find in this period, try last period # seldom do this
      param['KeyConditionExpression'] = Key('lnk_date_').eq((self.chain_no << 32) | (tm-31104000))
      res = self._safe_query(self.tb_blocks,**param)
    
    if not res:
      return None
    else: return res[0]
  
  def get_last_catchup(self, guest_hi):
    m = guest_hi % 103680   # 12 * 24 * 360 = 103680, about one year, release 12 blocks per hour
    guest_hi -= m
    
    param = { 'TableName':'utxos', 'IndexName':'lnk_height-height-index',
      'Limit':1, 'ScanIndexForward':False,
      'KeyConditionExpression': Key('lnk_height_').eq((self.chain_no << 32) | guest_hi),
    }
    res = self._safe_query(self.tb_utxos,**param)
    
    if not res and type(res) == list:  # not find in this period, try last period # seldom do this
      param['KeyConditionExpression'] = Key('lnk_height_').eq((self.chain_no << 32 ) | (guest_hi-103680))
      res = self._safe_query(self.tb_utxos,**param)
    
    if not res:
      return None
    else: return res[0]
  
  def renew_db(self):
    import boto3
    dynDb = boto3.resource('dynamodb',region_name=os.environ.get('AWS_REGION'))
    
    dynDb.create_table( TableName='blocks',
      KeySchema=[{'AttributeName':'lnk_id_', 'KeyType':'HASH'}],
      AttributeDefinitions=[ {'AttributeName':'hash_', 'AttributeType':'B'},
        {'AttributeName':'height_', 'AttributeType':'N'},
        {'AttributeName':'lnk_date_', 'AttributeType':'N'},
        {'AttributeName':'lnk_height_', 'AttributeType':'N'},
        {'AttributeName':'lnk_id_', 'AttributeType':'N'},
        {'AttributeName':'miner_hint_', 'AttributeType':'N'},
        {'AttributeName':'prev_lnk_id_', 'AttributeType':'N'} ],
      GlobalSecondaryIndexes=[
        {'IndexName':'lnk_date-lnk_height-index', 'KeySchema': [
            {'AttributeName':'lnk_date_', 'KeyType':'HASH'},
            {'AttributeName':'lnk_height_', 'KeyType':'RANGE'}
          ], 'Projection': {'ProjectionType':'ALL'} },
        {'IndexName':'miner_hint-height-index', 'KeySchema': [
            {'AttributeName':'miner_hint_', 'KeyType':'HASH'},
            {'AttributeName':'height_', 'KeyType':'RANGE'}
          ], 'Projection': {'ProjectionType':'ALL'} },
        {'IndexName':'lnk_height-index', 'KeySchema': [
            {'AttributeName':'lnk_height_', 'KeyType':'HASH'}
          ], 'Projection': {'ProjectionType':'ALL'} },
        {'IndexName':'hash-index', 'KeySchema': [
            {'AttributeName':'hash_', 'KeyType':'HASH'}
          ], 'Projection': {'ProjectionType':'ALL'} },
        {'IndexName':'prev_lnk_id-index', 'KeySchema': [
            {'AttributeName':'prev_lnk_id_', 'KeyType':'HASH'}
          ], 'Projection': {'ProjectionType':'ALL'} } ],
      BillingMode='PAY_PER_REQUEST' )
    
    dynDb.create_table( TableName='txns',
      KeySchema=[{'AttributeName':'lnk_id_', 'KeyType':'HASH'}, 
        {'AttributeName':'position_', 'KeyType':'RANGE'} ], 
      AttributeDefinitions=[{'AttributeName':'hash_', 'AttributeType':'B'}, 
        {'AttributeName':'lnk_id_', 'AttributeType':'N'},
        {'AttributeName':'position_', 'AttributeType':'N'} ],
      GlobalSecondaryIndexes=[
        {'IndexName':'hash-index', 'KeySchema': [
            {'AttributeName':'hash_', 'KeyType':'HASH'}
          ],'Projection': {'ProjectionType':'ALL'}} ],
      BillingMode='PAY_PER_REQUEST' )
    
    dynDb.create_table( TableName='utxos',
      KeySchema=[{'AttributeName':'lnk_id_', 'KeyType':'HASH'}, 
        {'AttributeName':'position_', 'KeyType':'RANGE'} ], 
      AttributeDefinitions=[{'AttributeName':'addr_', 'AttributeType':'B'},
        {'AttributeName':'lnk_id_', 'AttributeType':'N'},
        {'AttributeName':'height_', 'AttributeType':'N'},
        {'AttributeName':'position_', 'AttributeType':'N'},
        {'AttributeName':'lnk_height_', 'AttributeType':'N'}, 
        {'AttributeName':'uock_', 'AttributeType':'N'} ],
      GlobalSecondaryIndexes=[
        {'IndexName':'lnk_height-height-index', 'KeySchema': [
            {'AttributeName':'lnk_height_', 'KeyType':'HASH'},
            {'AttributeName': 'height_', 'KeyType': 'RANGE'}
          ], 'Projection': {'ProjectionType':'ALL'} }, 
        {'IndexName': 'addr-uock-index', 'KeySchema': [
            {'AttributeName':'addr_', 'KeyType':'HASH'}, 
            {'AttributeName':'uock_', 'KeyType':'RANGE'}
          ], 'Projection': {'ProjectionType':'ALL'} } ],
      BillingMode='PAY_PER_REQUEST' )
    
    dynDb.create_table( TableName='states',
      KeySchema=[{'AttributeName':'H', 'KeyType':'HASH'},
        {'AttributeName':'R', 'KeyType':'RANGE'} ],
      AttributeDefinitions=[{'AttributeName':'H', 'AttributeType':'S'},
        {'AttributeName':'R', 'AttributeType':'N'},
        {'AttributeName':'time_', 'AttributeType':'N'} ],
      LocalSecondaryIndexes=[
        {'IndexName':'H-time-index', 'KeySchema': [
            {'AttributeName':'H', 'KeyType':'HASH'}, 
            {'AttributeName':'time_', 'KeyType':'RANGE'}
          ], 'Projection': {'ProjectionType':'ALL'} } ],
      BillingMode='PAY_PER_REQUEST' )
  
  def s3_upload_db(self, local_dir, s3alias='sqlite'):
    if self.s3db_bucket and self.s3db_path:
      return _s3_upload_db(local_dir,self.s3db_bucket,os.path.join(self.s3db_path,s3alias))
    else: return 0
  
  def s3_download_db(self, local_dir, s3alias='sqlite'):
    if self.s3db_bucket and self.s3db_path:
      return _s3_download_db(local_dir,self.s3db_bucket,os.path.join(self.s3db_path,s3alias))
    else: return 0
  
  def init_db(self, node):
    global s3, dynamodb, Key, Attr
    if dynamodb: return
    
    import boto3      # should run boto3 under python3.4+
    from boto3.dynamodb import conditions
    Key  = conditions.Key
    Attr = conditions.Attr
    dynamodb = boto3.resource('dynamodb',region_name=os.environ.get('AWS_REGION')) # region_name can be None
    
    # not use: s3 = boto3.client('s3')  # it depends AWS_SESSION_TOKEN that applied max for 36 hours
    s3 = boto3.resource('s3',region_name=os.environ.get('AWS_REGION'))
    self.s3db_bucket = os.environ.get('AWS_DB_BUCKET','nb-filedb')
    self.s3db_path = os.environ.get('AWS_DB_PATH','txn_files')
    
    self.tb_blocks = dynamodb.Table('blocks')
    self.tb_txns   = dynamodb.Table('txns')
    self.tb_utxos  = dynamodb.Table('utxos')
    self.tb_states = dynamodb.Table('states')
    
    self._node = node
    self._coin = node.coin
    self._blocks = node._blocks
    self._chain_no = node._chain_no
    
    item = self.get_last_block()
    if item:
      self._last_height = int(item.get('lnk_height_',0)) & 0xffffffff  # lnk_height_: linkNo(16) + height(32)
    # else, self._last_height = 0
    cloud_last_hi = self._last_height
    
    item = self.get_last_catchup(max(0,self._last_height))
    if item:
      self._last_valid_id = int(item.get('lnk_id_',0)) & 0xffffffff    # lnk_id_: linkNo(16) + blockId(32)
      self._last_valid_hi = int(item.get('height_',0))
    # else, self._last_valid_id = 0, self._last_valid_hi = -1
    
    self._state = 'local_sync'
    print('DB backup starting...')
    print(self.state())
    
    # scan local files: zip/00000000-504.zip ...
    zip_dir = os.path.join(node._blocks.data_dir,'zip')
    zipList = block_packer.init_local(zip_dir,self.chain_no)
    
    # download zip/xxx-xxx.zip from S3
    from_hi = (zipList[-1][0] + zipList[-1][1]) if zipList else 0
    old_zips = len(zipList)
    down_zipped_file(zipList,self.s3db_bucket,self.s3db_path,zip_dir,from_hi)
    print('zip files: old=%i, new=%i, from_hi=%i' % (old_zips,len(zipList)-old_zips,from_hi))
    
    if zipList:
      block_packer._last_local = zipList[-1]
    
    # load blocks from local zip files
    _unspent = self._node._unspent
    prev_hash = _unspent._last_valid.hash
    hi2 = _unspent._last_valid.height + 1
    # first_start = hi2 == 1
    try:
      for (hi_,num_) in zipList:
        hi2,prev_hash = self.sync_from_zip(hi_,num_,hi2,prev_hash,zip_dir)
        if not prev_hash: break
        if self._node._exited: break
    except:
      traceback.print_exc()
    
    # try sync from cloud db directly
    if _unspent._last_valid.height == 0:  # load nothing from local zip file
      if self._blocks._topmost.height == 0:  # local db in initial state, from beginning
        print('an empty sqlite3 DB starting ...')
        
        # if self.sync_from_cloud():      # sync from cloud (not safe) unsupport, load from zip is more effective
        #   self._state = 'cloud_sync'
        
        if cloud_last_hi > 0:
          print('! fatal error: cloud db of backup target is not empty')
          return       # not start thread, system will hang off, death loop waiting
      
      # self._state = 'local_sync'        # keep local_sync state
    
    else:
      bkid = self._last_valid_id
      if bkid and _unspent._last_valid._blockid > bkid:
        while True:   # ensure bkid in mainchain, otherwise try previous
          bk = self._block._get(bkid)
          if not bk:
            self._last_valid_id = 0
            self._last_valid_hi = 0
            break
          elif bk.mainchain:
            self._last_valid_id = bkid  # re-catch-up from this block
            self._last_valid_hi = bk.height
            break
          else: bkid = bk._previous_blockid
    
    # start thread of DbBackup
    self.start()
  
  def exit(self):
    self._active = False
    self.join()
  
  def state(self):
    return '%s: last=%i, valid=%i, task=%i' % (self._state,self._last_height,self._last_valid_hi,len(self._tasks))
  
  def truncate_blocks(self, hi, tm):  # remove all height_ >= hi items of self.tb_blocks
    m = tm % 31104000  # about one year
    nearPeriod = False
    if m:
      if m > 31089600: # within 4 hours, 31104000 - 14400 = 31089600
        nearPeriod = True
      tm -= m
    
    param = { 'TableName':'blocks','IndexName':'lnk_date-lnk_height-index',
      'ProjectionExpression':'lnk_id_', 'ScanIndexForward':False,
    }
    
    try:
      if nearPeriod:
        tm2 = tm + 31104000
        param['KeyConditionExpression'] = Key('lnk_date_').eq((self.chain_no << 32) | tm2) & Key('lnk_height_').gte((self.chain_no << 32) | hi)
        res = self._safe_query(self.tb_blocks,**param)
        if res:
          with self.tb_blocks.batch_writer() as batch:
            for item in res:
              batch.delete_item(Key={'lnk_id_':int(item['lnk_id_'])})
      
      param['KeyConditionExpression'] = Key('lnk_date_').eq((self.chain_no << 32) | tm) & Key('lnk_height_').gte((self.chain_no << 32) | hi)
      res = self._safe_query(self.tb_blocks,**param)
      if res:
        with self.tb_blocks.batch_writer() as batch:
          for item in res:
            batch.delete_item(Key={'lnk_id_':int(item['lnk_id_'])})
    except: # if meet any error, just ignore
      traceback.print_exc()
  
  def process_task(self, oneTask):
    sCmd = oneTask[0]
    if sCmd == '@valid':
      self._last_valid_id = oneTask[1]
      
      hi = oneTask[2]
      self._last_valid_hi = hi
      if hi > self._last_height:
        self._last_height = hi
    
    elif sCmd == '+blocks':
      v = oneTask[1]; bk_id = v[0]; bk_hi = v[12]
      tm = v[6]; m = tm % 31104000
      m = (tm - m) if m else tm
      
      param = { 'Item': {
        'lnk_id_': (self.chain_no << 32) | bk_id,
        'prev_lnk_id_': v[1],
        'hash_': v[2],
        'version_': v[3],
        'link_no_': v[4],
        'merkle_': v[5],
        'timestamp_': tm,
        'lnk_date_': m,
        'bits_': v[7],
        'nonce_': v[8],
        'miner_': v[9],
        'sig_tee_': v[10],
        'txn_count_': v[11],
        'lnk_height_': (self.chain_no << 32) | bk_hi,
        'txn_ready_': v[13],
        'mainchain_': v[14],
        'miner_hint_': v[15],
      }}
      if not self._safe_put_item(self.tb_blocks,**param):  # meet error
        return False
      
      if bk_hi > self._last_height:
        self._last_height = bk_hi
      
      print('DB backup add new block header (hi=%i)' % (bk_hi,))
    
    elif sCmd == '@blocks':
      expr,value = get_set_expr(oneTask[2].items())
      param = { 'Key': {'lnk_id_': oneTask[1]},
        'UpdateExpression': expr,
        'ExpressionAttributeValues': value,
      }
      if not self._safe_update(self.tb_blocks,**param):  # meet error
        return False
    
    elif sCmd == '-blocks':
      param = { 'Key': {'lnk_id_':oneTask[1]} }
      if not self._safe_remove(self.tb_blocks,**param):  # meet error
        return False
    
    elif sCmd == '+txns':
      (hash_,hi,txck,mainchain) = oneTask[1:5]
      param = { 'Item': {
        'lnk_id_': (self.chain_no << 32)|(txck >> 20),
        'position_': txck & 0xfffff,
        'hash_': hash_,
        'mainchain_': mainchain,
        'height_': hi,
      }}
      if not self._safe_put_item(self.tb_txns,**param):  # meet error
        return False
    
    elif sCmd == '@txns':
      txck = oneTask[1]
      expr,value = get_set_expr(oneTask[2].items())
      param = { 'Key': {'lnk_id_':(self.chain_no << 32)|(txck >> 20), 'position_':txck & 0xfffff},
        'UpdateExpression': expr,
        'ExpressionAttributeValues': value,
      }
      if not self._safe_update(self.tb_txns,**param):  # meet error
        return False
    
    elif sCmd == '@txns1':
      txck_from = oneTask[1]
      lnk_id = (self.chain_no << 32) | (txck_from >> 20)
      
      param = {
        'KeyConditionExpression': Key('lnk_id_').eq(lnk_id),
      }
      res = self._safe_query_count(self.tb_txns,**param)
      if type(res) != int: return False # meet error
      
      if res > 0:  # has 1+ txns under this block(lnk_id)
        expr,value = get_set_expr(oneTask[2].items())
        try:
          for i in range(res):
            param2 = { 'Key':{'lnk_id_':lnk_id,'position_':i},
              'UpdateExpression':expr, 'ExpressionAttributeValues':value }
            if not self._safe_update(self.tb_txns,**param2):
              return False
        except:  # if meet any error, just quit thread
          traceback.print_exc()
          return False
    
    elif sCmd == '-txns1':
      txck_from = oneTask[1]
      lnk_id = (self.chain_no << 32) | (txck_from >> 20)
      
      param = {
        'KeyConditionExpression': Key('lnk_id_').eq(lnk_id),
      }
      res = self._safe_query_count(self.tb_txns,**param)
      if type(res) != int: return False   # meet error
      
      if res > 0:  # has 1+ txns under this block(lnk_id)
        try:
          with self.tb_txns.batch_writer() as batch:
            for i in range(res):
              batch.delete_item(Key={'lnk_id_':lnk_id,'position_':i})
        except:    # if meet any error, just quit thread
          traceback.print_exc()
          return False
    
    elif sCmd == '+utxos':   # ignore duplicate
      tx_value = oneTask[1]; mature_hi = oneTask[2]; row = oneTask[3]
      uock = row[0]
      lnk_id = (self.chain_no << 32) | (uock >> 40)
      out_idx = uock & 0xfffff
      txn_idx = (uock >> 20) & 0xfffff
      
      hi = row[1]
      m = hi % 103680  # 12 * 24 * 360 = 103680  # release 12 blocks in one hour
      hi -= m
      
      param = { 'Item': {
        'lnk_id_': lnk_id,
        'position_': (txn_idx << 32) | out_idx,
        'height_': row[1],
        'lnk_height_': hi,
        'uock_': uock,
        'addr_': row[2],          # public_hash + coin_type
        'vcn_': row[4],           # vcn of address
        'coin_type_': row[5],
        'value_': tx_value,
        'mature_hi_': mature_hi,  # if -1 means no maturity limit
      }}
      if not self._safe_put_item(self.tb_utxos,**param):  # meet error
        return False
    
    elif sCmd == '-utxos':
      uock = oneTask[1]
      lnk_id = (self.chain_no << 32) | (uock >> 40)
      out_idx = uock & 0xfffff
      txn_idx = (uock >> 20) & 0xfffff
      
      param = {
        'Key': {'lnk_id_':lnk_id,'position_':(txn_idx << 32) | out_idx},
      }
      if not self._safe_remove(self.tb_utxos,**param):
        return False  # table.delete_item() is idempotent operation
    
    elif sCmd == '-utxos1':
      txck_from = oneTask[1]
      lnk_id = (self.chain_no << 32) | (txck_from >> 20)
      param = {
        'KeyConditionExpression': Key('lnk_id_').eq(lnk_id),
      }
      res = self._safe_query(self.tb_utxos,**param)  # only get 'position_'
      if type(res) != list: return False # meet error
      
      if res:
        try:
          with self.tb_utxos.batch_writer() as batch:
            for item in res:
              batch.delete_item(Key={'lnk_id_':lnk_id,'position_':int(item['position_'])})
        except:    # if meet any error, just quit thread
          traceback.print_exc()
          return False
    
    elif sCmd == '[txns]':
      bk_hi = oneTask[1]; bk = oneTask[2]
      if not bk: bk = self._blocks._get2(bk_hi)
      hash_str = hexlify(bk.hash)
      head_bin = _format.BlockHeader.from_block(bk).binary()
      
      output = b'index,hash,content\r\n'
      output += b'%i,%s,%s\r\n' % (-1,hash_str,base64.b64encode(head_bin))
      for (idx,txn) in enumerate(bk.txns):
        output += b'%i,%s,%s\r\n' % (idx,hexlify(txn.hash),base64.b64encode(txn.txn_bin))
      
      succ = True
      sPath = os.path.join(self.s3db_path,'%i-%i.csv' % (self.chain_no,bk_hi))
      meta = { 'hash':hash_str.decode('latin-1'),
        'txn': str(len(bk.txns)), 'id': str(bk._blockid) }
      for i in range(3):
        try:
          s3.Bucket(self.s3db_bucket).put_object(Key=sPath,Body=output,Metadata=meta)
          break
        except:
          if i == 2:
            traceback.print_exc()
            succ = False
      if succ:
        self._last_backup_hi = bk_hi
        print('Backup and upload one block (hi=%i)' % (bk_hi,))
        
        if (bk_hi % 504) == 0:
          pack_from = block_packer._last_local
          if pack_from:
            pack_from = pack_from[0] + pack_from[1]
          else: pack_from = 0
          
          # backup previous period, the most recent block still in blocks switching
          bk_hi -= 504
          if bk_hi > pack_from:
            pack_num = bk_hi - pack_from
            if block_packer.pack_upload_zip(pack_from,pack_num,self._node._blocks,self.s3db_bucket,self.s3db_path):
              print("pack and upload '%08i-%i.zip' successful" % (pack_from,pack_num))
            else: print("warning: pack and upload '%08i-%i.zip' and upload failed" % (pack_from,pack_num))
      else:
        self.truncate_blocks(bk_hi,bk.timestamp)  # truncate should be safe # system will retry next time
        return False    # quit current thread
    
    elif sCmd == '-pool':
      hash_ = oneTask[1]
      now = int(time.time()); year = now // 31536000
      param = { 'Item': {
        'H': 'RMV_TX#' + str(year),
        'R': now,
        'hash_': oneTask[1],
      }}
      self._safe_put_item(self.tb_states,**param)
    
    else: print('warning: unknown command (%s)' % (sCmd,))
    
    return True
  
  def catchup_block(self):
    if self._last_valid_id == 0:
      bk = self._blocks._get2(0)  # get next block by height
      if not bk: return False
    else:
      bk = self._blocks.get_nextmain(self._last_valid_id)
      if not bk: return False     # nothing to catch
    
    if bk.height > self._node._unspent._last_valid.height:
      return False
    if not bk.txn_ready: return False  # just double check
    if not bk.mainchain: return False  # just double check
    
    # backup block head
    bak_task = []
    bak_task.append( ('+blocks', (bk._blockid,  # v[0]
        bk._previous_blockid, # v[1]
        bk.hash,         # v[2]
        bk.version,      # v[3]
        bk.link_no,      # v[4]
        bk.merkle_root,  # v[5]
        bk.timestamp,    # v[6]
        bk.bits,         # v[7]
        bk.nonce,        # v[8]
        bk.miner,        # v[9]
        bk.sig_tee,      # v[10]
        bk.txn_count,    # v[11]
        bk.height,       # v[12]
        bk.txn_ready,    # v[13]
        bk.mainchain,    # v[14]
        bk.miner_hint))) # v[15]
    
    # backup txns
    txns = bk.txns
    for txn in txns:
      txn.cache_prev_outputs()   # prepare txn._po_cache={inputIndex:txn}
      bak_task.append(('+txns',txn.hash,bk.height,txn._txck,bk.mainchain))
    bak_task.append(('[txns]',bk.height,bk))
    
    # backup utxos
    txns_addrs = []
    for (i,txn) in enumerate(txns):
      if i == 0:  # it is coinbase
        txns_addrs.append(None)
        
        # add utxo of coinbase   # assert(len(txn.outputs) == 2)
        addr = bk.miner
        addr_hint = keys.get_hint(addr)
        
        for idx in range(2):
          uock = keys.get_uock(txn._txck,idx)
          coin_type = self._coin.mining_coin_type if idx == 0 else self._coin.currency_coin_type
          row = (uock,bk.height,buffer(addr+coin_type),addr_hint,bk.link_no&0xffff,buffer(coin_type))
          bak_task.append(('+utxos',txn.outputs[idx].value,bk.height,row))
      
      else:
        txio = script.Script(txn,bk.height,self._coin)
        txio_addrs = [txio.output_addr(i2,self._blocks) for i2 in range(0,txio.output_count)]
        txns_addrs.append(txio_addrs)  # address should be vcn2+hash32+cointype or None
    
    for i in range(1,len(txns)):
      # remove each input's previous UTXO
      txn = txns[i]
      for idx in range(0,len(txn.inputs)):
        uock = txn._previous_uock(idx)
        if uock is not None:
          bak_task.append(('-utxos',uock))
      
      # add each output's UTXO
      addrs = txns_addrs[i]
      for (idx,addr) in enumerate(addrs):   # addr is vcn2+hash32+cointype
        if (not addr) or len(addr) <= 34: continue  # addr can be None when output value is 0
        
        uock = keys.get_uock(txn._txck,idx)
        addr_ = addr[2:]
        addr_hint = keys.get_hint(addr_)
        addr_vcn = (ORD(addr[1]) << 8) + ORD(addr[0])
        
        row = (uock,bk.height,buffer(addr_),addr_hint,addr_vcn,buffer(addr_[32:]))
        bak_task.append(('+utxos',txn.outputs[idx].value,-1,row))
    
    bak_task.append(('@valid',bk._blockid,bk.height)) # this is last one
    for task in bak_task:
      if not self.process_task(task):
        return False
    
    return True
  
  def save_exist_block(self):
    hi = self._last_height + 1
    bk = self._blocks._get2(hi)
    if not bk: return False
    
    if not self.process_task( ('+blocks', ( bk._blockid,  # v[0]
        bk._previous_blockid, # v[1]
        bk.hash,         # v[2]
        bk.version,      # v[3]
        bk.link_no,      # v[4]
        bk.merkle_root,  # v[5]
        bk.timestamp,    # v[6]
        bk.bits,         # v[7]
        bk.nonce,        # v[8]
        bk.miner,        # v[9]
        bk.sig_tee,      # v[10]
        bk.txn_count,    # v[11]
        bk.height,       # v[12]
        bk.txn_ready,    # v[13]
        bk.mainchain,    # v[14]
        bk.miner_hint)) ): # v[15]
      return False
    
    self._last_height = bk.height  # bk.txn_ready can be False
    return True
  
  def sync_from_zip(self, hi, num, from_hi, prev_hash, zip_dir):
    if from_hi < hi: return (from_hi,b'')    # means error  # has finished
    if from_hi >= hi + num: return (from_hi,prev_hash)      # waiting next call
    
    meetErr, prev_hash = load_zipped_block(self._node,prev_hash,from_hi,hi,num,zip_dir,self.chain_no)
    
    self._last_valid_id = self._node._unspent._last_valid._blockid
    self._last_valid_hi = self._last_height = self._node._unspent._last_valid.height
    print('... end sync (hi=%i)' % (self._last_height,))
    
    return (self._last_height+1, b'' if meetErr else prev_hash)
  
  def sync_from_cloud(self):
    _blocks = self._blocks
    _txns = self._node._txns
    _unspent = self._node._unspent
    
    _unspent.catch_up_check(self._node)  # try catchup
    
    meetErr = False; counter = 0
    while _unspent._last_valid._blockid < self._last_valid_id:
      prev_hash = _unspent._last_valid.hash
      hi = _unspent._last_valid.height + 1
      sPath = os.path.join(self.s3db_path,'%i-%i.csv' % (self.chain_no,hi))
      
      income = BytesIO()
      try:
        b = []; meta = {}
        try:
          # s3.Bucket(self.s3db_bucket).download_fileobj(sPath,income)
          res = s3.Object(self.s3db_bucket,sPath).get()
          if res['ResponseMetadata']['HTTPStatusCode'] == 200:
            meta = res['Metadata']
            b = _re_newline.split(res['Body'].read())
          else:    # try again
            time.sleep(2)
            res = s3.Object(self.s3db_bucket,sPath).get()
            if res['ResponseMetadata']['HTTPStatusCode'] == 200:
              meta = res['Metadata']
              b = _re_newline.split(res['Body'].read())
        except:
          break    # if download failed, just ignore, maybe file inexistent
        
        bk_id = meta.get('id')
        if not bk_id:
          meetErr = True
          break
        bk_id = int(bk_id)
        
        if b: del b[0]  # remove header line
        if len(b) < 2:  # at least have: head, one txn
          meetErr = True
          break
        
        b2 = b[0].split(b',')  # idx,hash,content
        head = _format.BlockHeader.parse(base64.b64decode(b2[2]))[1] 
        del b[0]
        
        txns = []
        for item in b:
          b2 = item.split(b',')
          if len(b2) == 3:
            txns.append(_format.Txn.parse(base64.b64decode(b2[2]))[1])
        if head.prev_block != prev_hash or head.txn_count != len(txns):
          print('sync from clould db failed at height (%i): mismatch' % (hi,))
          break
        
        msg_block = protocol.Block( head.version, head.link_no, head.prev_block,
          head.merkle_root, head.timestamp, head.bits,
          head.nonce, head.miner, head.sig_tee, txns )
        
        curr_block = None
        try:
          curr_block = _blocks.add_header(self._node,msg_block.make_header(),False,head.hash,fix_id=bk_id) # False means no-check-duplicate
          _txns.add(self._node,curr_block,txns) # add transactions
        except InvalidBlockException as e:
          if curr_block:
            _blocks.reject_block(curr_block)
          raise e;
        
        if curr_block:
          if head.hash == curr_block.hash:
            print('sync from cloud db successful: %i' % (hi,))
          
          if curr_block.mainchain:
            _unspent.catch_up_check(self._node,curr_block)
          else: break   # meet unexpected error, quit cloud db sync
        else: break     # meet unexpected error, quit cloud db sync
        
        counter += 1
        if counter % 10 == 9:   # not too hurry
          time.sleep(1)
      except:
        traceback.print_exc()
        meetErr = True
        break   # quit loop
    
    if meetErr: return False
    
    self._last_valid_id = _unspent._last_valid._blockid
    self._last_valid_hi = self._last_height = _unspent._last_valid.height
    return True
  
  def run(self):
    self._active = True
    while self.is_alive() and self._active:
      if self._node._exited: break
      
      if self._state == 'local_sync':
        catched_any = False
        for ii in range(100):
          if self._node._unspent._last_valid._blockid > self._last_valid_id:
            if self.catchup_block():
              catched_any = True
              time.sleep(0.2)     # not so hurry, suite for cloud db capacity
            else: break           # catch up one block
          else: break
        
        if not catched_any:
          _bkid = self._node._unspent._last_valid._blockid
          if _bkid == self._last_valid_id:
            for ii in range(100):
              if self._blocks._topmost.height > self._last_height: # _last_height >= 0 since first block will auto-catchup
                if self.save_exist_block():
                  catched_any = True
                  time.sleep(0.2)      # not so hurry, suite for cloud db capcity
                else: break
              else:
                if self._blocks._topmost.height == self._last_height:
                  self._state = 'cloud_sync'
                break
          elif _bkid < self._last_valid_id:
            self._state = 'cloud_sync'  # node.serve_forever() would be holding till now
          # else, continue
        
        if not catched_any: time.sleep(5)
        continue
      
      if self._state != 'cloud_sync': break
      
      succ = True
      while self._active:
        try:
          oneTask = self._tasks.pop()
        except:            # ignore IndexError
          oneTask = None
        if not oneTask: break
        
        if self.process_task(oneTask):
          time.sleep(1)
        else:
          succ = False
          break
      
      if not succ: break   # meet unrecover error
      time.sleep(5)
    
    self._state = 'exited'
    self._active = False
    print('DbBackup thread exited.')
    
    if not self.readonly:
      if not self._node._exited:
        try:
          self._node.close()
          time.sleep(10)
        except: pass
      if self.ec2_reboot: self.ec2_reboot()  # try restart EC2 instance
  
  def add_task(self, task):
    if self._active and self._state == 'cloud_sync':
      self._tasks.insert(0,task)
  
  def try_wait_task(self):
    if self._active and self._state == 'cloud_sync':
      task_num = len(self._tasks)
      if task_num > self.LOW_BACKUP_TASK and current_thread() != self:
        if task_num > self.MAX_BACKUP_TASK:
          time.sleep(40)     # not too long time, avoid holding asyncore thread
        elif task_num > self.MID_BACKUP_TASK:
          time.sleep(20)
        else: time.sleep(10)

db_backup = DbBackup()

#------------

def make_db_file(data_dir, coin_name, db_name, extra=''):
  return os.path.join(data_dir,'%s-%s%s.sqlite' % (coin_name,db_name,extra))

class Database(object):
  # Columns is: [(column_name,column_type,should_index)...]
  Columns = []         # will set by subclass
  Name = 'table_name'  # will set by subclass
  
  # denote non-backwards-compatible changes
  Version = 1
  
  def __init__(self, data_dir=None, coin=coins.Newbitcoin):
    if data_dir is None:
      data_dir = util.default_data_dir()
    self.__data_dir = data_dir
    self.__coin = coin
    
    self.sql_select = 'select %s from %s' % (','.join(n for (n,t,i) in self.Columns),self.Name)
    
    offset = 0
    if len(self.Columns) and self.Columns[0][0] == 'id':
      offset = 1
    self.sql_insert = 'insert into %s (%s) values (%s)' % ( self.Name,
      ','.join(n for (n,t,i) in self.Columns[offset:]),
      ','.join('?' for c in self.Columns[offset:]) )
  
  data_dir = property(lambda s: s.__data_dir)
  coin = property(lambda s: s.__coin)
  
  def get_filename(self, extra=''):
    return make_db_file(self.data_dir,self.coin.name,self.Name,extra)
  
  def get_connection(self, extra='', iso_level=''):
    filename = self.get_filename(extra)
    ensure_dir_(os.path.split(filename)[0])
    
    # connection = sqlite3.connect(filename,timeout=30,isolation_level=iso_level) # isolation_level='' means smart commit
    connection = sqlite3.connect(filename,timeout=30,isolation_level=iso_level,check_same_thread=False)
    connection.row_factory = sqlite3.Row
    self.init_database(connection)
    return connection
  
  def init_database(self, connection):
    cursor = connection.cursor()
    
    try:
      # create a metadata table to track version and custom keys/values
      sql = 'create table metadata (key integer primary key, value integer, value_bin blob)'
      cursor.execute(sql)
      
      # set the database version
      sql = 'insert into metadata (key, value, value_bin) values (?, ?, ?)'
      cursor.execute(sql,(KEY_VERSION,self.Version,None))
      
      # create table
      column_defs = ','.join('%s %s' % (n,t) for (n,t,i) in self.Columns)
      sql = 'create table %s (%s)' % (self.Name,column_defs)
      cursor.execute(sql)
      
      # add index
      for (n,t,i) in self.Columns:
        if not i: continue
        sql = "create index index_%s on %s (%s)" % (n, self.Name, n)
        cursor.execute(sql)
      
      # let subclasses pre-populate the database
      self.populate_database(cursor)
      
      connection.commit()
    
    except sqlite3.OperationalError as e:  # it will happen every time except first
      if str(e) != ('table metadata already exists'):
        raise e
      
      version = self.get_metadata(cursor,KEY_VERSION)
      if version != self.Version:  # check compatible
        raise DbException('incompatible database version: %d (expected %d)' % (version,self.Version))
  
  def set_metadata(self, cursor, key, value): # the value may be integer or string
    if key == KEY_VERSION:
      raise ValueError('cannot change version')
    
    sql = 'insert or replace into metadata (key, value, value_bin) values (?, ?, ?)'
    if isinstance(value,int):
      cursor.execute(sql,(key,value,None))
    elif isinstance(value,str):
      cursor.execute(sql,(key,0,buffer(value)))
    else:
      raise ValueError('value must be integer or string')
  
  def get_metadata(self, cursor, key, default=None):
    sql = 'select value, value_bin from metadata where key = ?'
    cursor.execute(sql,(key,))
    row = cursor.fetchone()
    if row:
      if row[1] is not None:
        return str(row[1])
      return row[0]
    else: return default
  
  def populate_database(self, cursor):  # waiting overwrite
    pass
