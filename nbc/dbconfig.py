
import sys, os, json

class DbConfig(object):
  def __init__(self, cfg_file=None):
    self.__dict = {}
    self._cfg_file = cfg_file
    self._prefixs  = ('ENV/','CFG/')
    
    if cfg_file:
      with open(cfg_file) as f:
        cfg = json.loads(f.read())
        for (key,value) in cfg.items():
          key = str(key)
          for s in self._prefixs:
            if key.find(s) == 0:
              self.__dict[key] = value
              break
  
  def save_file(self, cfg_file, by_this_file=False):
    with open(cfg_file,'w') as f:
      f.write(json.dumps(self.__dict))
      if by_this_file: self._cfg_file = cfg_file
  
  def __save(self):
    if self._cfg_file:
      with open(self._cfg_file,'w') as f:
        f.write(json.dumps(self.__dict))
  
  def __getitem__(self, name):
    return self.__dict[name]
  
  def __setitem__(self, name, value):
    for s in self._prefixs:
      if name.find(s) == 0:
        self.__dict[name] = value
        self.__save()
        return value
    raise KeyError('unknown prefix of key (%s)' % (name,))
  
  def __delitem__(self, name):
    del self.__dict[name]
    self.__save()
  
  def __len__(self):
    return len(self.__dict)
  
  def __in__(self, name):
    return name in self.__dict
  
  def __iter__(self):
    return iter(self.__dict)
  
  def get(self, name, default=None):
    return self.__dict.get(name,default)
  
  def load_env(self):
    # prefix is: {'-ENV':'ENV/', '-CFG':'CFG/'}
    prefix2 = [s.replace('/','_') for s in self._prefixs]
    
    changed = False
    for (key,value) in os.environ.items():
      for (idx,prefix) in enumerate(prefix2):
        if key.find(prefix) == 0:
          self.__dict[self._prefixs[idx]+key[len(prefix):]] = value
          changed = True
          break
    if changed: self.__save()
  
  def load_argv(self):
    # prefix is: {'-ENV':'ENV/', '-CFG':'CFG/'}
    prefix = dict(('-' + s.replace('/',''),s) for s in self._prefixs)
    
    args = sys.argv[1:]
    curr_prefix = ''; changed = False
    for arg in args:
      if arg[0] == '-':
        curr_prefix = prefix.get(arg) or ''
      else:
        if curr_prefix:
          b = arg.split('=')
          self.__dict[curr_prefix+b[0]] = b[1] if len(b) >= 2 else ''
          changed = True
        # else, ignore
    
    if changed: self.__save()

config = DbConfig()
