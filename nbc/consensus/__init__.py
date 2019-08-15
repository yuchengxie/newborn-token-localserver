
import time, random, struct, hashlib
from binascii import hexlify
from threading import Thread

__all__ = [
  'init_tee', 'poet_sign', 'poet_verify', 'pow_verify', 'PowThread',
]

import six

if six.PY2:
  def make_long(s,base):
    return long(s,base)
else:
  def make_long(s,base):
    return int(s,base)

POET_ACCOUNT = None    # POET_ACCOUNT.sign(data) POET_ACCOUNT.verify(data,sig)
POET_PUBADDR = b'\x00' * 32

def init_tee(poet=None):   # pass poet for simulating
  global POET_ACCOUNT, POET_PUBADDR
  if poet:
    POET_ACCOUNT  = poet
    POET_PUBADDR  = poet.publicKey()[1:]  # first byte is flag (compress or not)

def poet_sign(block_hash, bits, txn_num, sig_flag, pub_key_hash):
  if POET_ACCOUNT:    # pub_key should be 32 bytes binary-string
    return POET_ACCOUNT.sign(block_hash + struct.pack('<II',bits,txn_num) + sig_flag + pub_key_hash) + sig_flag
  else: return None   # failed

def poet_verify(block_hash, bits, txn_num, pub_key_hash, sig):
  if POET_ACCOUNT:    # pub_key should be 32 bytes binary-string
    sig_flag = sig[-1:]   # the last byte is signature flag that indicates who is verificator
    return POET_ACCOUNT.verify(block_hash + struct.pack('<II',bits,txn_num) + sig_flag + pub_key_hash,sig[:-1])
  else: return False

#-------- POW ---------
MIN_POW_TARGET = make_long('F'*59,16)   # 'F'*58 about 160 seconds for PC, 40 seconds when using '3'+('F'*58)

def uint32(x):
  return x & 0xffffffff

def byteReverse(x):
  return uint32( (x<<24) | ((x<<8)&0x00ff0000) | ((x>>8)&0x0000ff00) | (x>>24) )

def bufReverse(in_buf):
  out_words = []
  for i in range(0, len(in_buf), 4):
    word = struct.unpack('@I', in_buf[i:i+4])[0]
    out_words.append(struct.pack('@I', byteReverse(word)))
  return b''.join(out_words)

def wordReverse(in_buf):
  out_words = []
  for i in range(0,len(in_buf),4):
    out_words.append(in_buf[i:i+4])
  out_words.reverse()
  return b''.join(out_words)

def isMeetTarget(static_hash, nonce):
  hash2 = static_hash.copy()
  hash2.update(struct.pack('<I',nonce))
  
  hash3 = hashlib.sha256(hash2.digest())
  hash4 = hash3.digest()
  if hash4[-2:] != b'\x00\x00':   # quick test
    return b''
  
  hash_ = wordReverse(bufReverse(hash4))
  if make_long(hexlify(hash_),16) < MIN_POW_TARGET:
    return hash4
  else: return b''

def pow_verify(block_hash, height=0):
  pow_targ = MIN_POW_TARGET  # block height no use yet, maybe from sometime on we should promote PoW
  hash_ = wordReverse(bufReverse(block_hash))
  return make_long(hexlify(hash_),16) < pow_targ

class PowThread(Thread):
  def __init__(self):
    Thread.__init__(self)
    self.daemon = True
    self._active = False
    
    self._cancelOne = False
    self._taskId = 0
    self._incoming = []
    self._output = []
  
  def exit(self):
    self._active = False
    self.join()    # wait forever
  
  def cancelCurr(self):
    self._cancelOne = True
    del self._incoming[:]
    del self._output[:]
  
  def solvePow(self, sHead):
    iRet = self._taskId + 1
    self._taskId = iRet
    
    del self._incoming[:]  # avoid adding two same task
    self._incoming.append((iRet,sHead))
    return iRet
  
  def getResult(self):
    ret = (False,0,0,b'')  # (succ,idx,nonce,hash)
    try:
      ret = self._output.pop()
    except: pass # IndexError as e
    return ret
  
  def run(self):
    self._active = True
    while self.is_alive() and self._active:
      if self._incoming:
        try:
          (idx,sHead) = self._incoming.pop()
        except: sHead = None
        if not sHead: continue
        
        self._cancelOne = False
        staticHash = hashlib.sha256(sHead)
        nonce = random.randint(0,0x10000000)
        
        tryCount = 0
        while self._active and not self._cancelOne:
          tryCount += 1
          nonce = (nonce + 1) & 0xffffffff
          retHash = isMeetTarget(staticHash,nonce)
          if retHash:      # success solved
            del self._output[:]
            self._output.append((True,idx,nonce,retHash))
            break
          else:
            if tryCount >= 1000:  # ensure low CPU occupy
              tryCount = 0
              time.sleep(0.1)
      
      else: time.sleep(2)  # nothing to process, go next loop
    
    self._active = False
    print('PoW thread exited.')

# import random, time
# from nbc import consensus
# 
# th = consensus.PowThread()
# th.start()
# 
# consensus.MIN_POW_TARGET = int('F'*58,16) # 29 bytes
# print('start:',time.time())
# idx = th.solvePow(b'example')
# while True:
#   ret,idx2,nonce = th.getResult()
#   if ret and idx == idx2:
#     print('get nonce:',nonce)
#     break
#   time.sleep(1)
# print('end:',time.time())
