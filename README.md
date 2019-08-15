node.close()
### About the project

We want to reconstruct bitcoin system, since it has been developped for 10 years.

The project still in developping, we are constructing from zero line of source code, and in first phase we will release a concise system that assume carry out functional verification.

The project is written in python, it supports Python2.7+ or Python3.4+

&nbsp;

### Install the project

Setup package not ready yet, please clone the project, and put it to python's library directory in your machine. 

Current version only can run in MAC OS.

&nbsp;

### The finished part

1) Nondeterministic wallet

Create wallet addresss:

``` python
from nbc.wallet import *
addr = Address.generate()
print(addr)
```

Get public key:

``` python
pubKey = addr.publicKey()
```

Save to file and load from file:

``` python
saveTo('default.txt',addr)
addr_ = loadFrom('default.txt')
print(addr_)
```

Save and load by encryption:

``` python
saveTo('default2.txt',addr,b'password')
addr2 = loadFrom('default2.txt',b'password')  # or: u'密码'.encode('utf-8')
print(addr2)

addr3 = loadFrom('default2.txt')   # system will prompt inputing when omit password
```

2) Deterministic wallet (HD wallet)

Create wallet addresss:

``` python
from nbc.wallet import *
addr = HDWallet.from_master_seed('HDWallet seed')
print(addr)
```

Extend key:

``` python
master = HDWallet.from_master_seed('HDWallet seed')
prvMaster = master.to_extended_key(include_prv=True)
pubMaster = master.to_extended_key()

webWallet = HDWallet.from_extended_key(pubMaster)
child2342 = webWallet.child(23).child(42)
```

Save to file and load from file:

``` python
saveTo('hd.txt',addr)
saveTo('hd2.txt',addr,b'password')

addr_ = loadFrom('hd.txt')
print(addr_)

addr2 = loadFrom('hd2.txt',b'password')
print(addr2)
```

3) Signature and verification for wallet keys

``` python
from nbc.wallet import *

addr = Address.generate()
ss = addr.sign(b'example')
addr.verify(b'example',ss)
addr.publicKey().verify(b'example',ss)

addr2 = HDWallet.from_master_seed('HDWallet seed')
ss2 = addr2.sign(b'example')
addr2.verify(b'example',ss2)
```

&nbsp;
