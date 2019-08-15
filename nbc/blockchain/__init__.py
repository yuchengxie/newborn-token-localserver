
from threading import RLock

from .. import coins
from .. import util

class InvalidBlockException(Exception): pass

from . import keys
from . import transaction
from . import block
from . import unspent

class DbLocker_(object):
  def __init__(self):
    self.lock = RLock()
  
  def enter(self):
    return self.lock.acquire()  # wait forever
  
  def leave(self):
    if self.lock._is_owned():
      try:
        self.lock.release()
      except: pass   # avoid report: cannot release un-acquired lock
  
  def wait_enter(self, tm=-1):  # tm=-1 means wait forever
    return self.lock.acquire(timeout=tm)

DbLocker = DbLocker_()
