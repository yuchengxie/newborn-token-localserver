from binascii import hexlify
import json
from nbc import script


class ModelReplyHeaders:
    state = 0
    link_no = 0
    heights = []
    txcks = []
    blocks = []

    @staticmethod
    def toDict(msg):
        dic = {}
        if msg == None:
            dic['state'] = 0
        dic['state'] = 1
        dic['link_no'] = msg.__getattribute__('link_no')
        dic['heights'] = list(msg.__getattribute__('heights'))
        dic['txcks'] = list(msg.__getattribute__('txcks'))
        # dic['txcks'] = msg.__getattribute__('txcks')
        headers = msg.__getattribute__('headers')
        bb = []
        for b in headers:
            s = ModelBlockHeader.toDict(b)
            bb.append(s)
        dic['headers'] = bb
        return dic


class ModelBlockHeader:
    version = 0
    link_no = 0
    prev_block = ''
    merkle_root = ''
    timestamp = 0
    bits = 0
    nonce = 0
    miner = ''
    sig_tee = ''
    txn_count = 0

    @staticmethod
    def toDict(msg):
        dic = {}
        dic['version'] = msg.__getattribute__('version')
        dic['link_no'] = msg.__getattribute__('link_no')
        dic['prev_block'] = (msg.__getattribute__('prev_block')).hex()
        dic['merkle_root'] = (msg.__getattribute__('merkle_root')).hex()
        dic['timestamp'] = msg.__getattribute__('timestamp')
        dic['bits'] = msg.__getattribute__('bits')
        dic['nonce'] = msg.__getattribute__('nonce')
        dic['miner'] = (msg.__getattribute__('miner')).hex()
        dic['sig_tee'] = (msg.__getattribute__('sig_tee')).hex()
        dic['txn_count'] = msg.__getattribute__('txn_count')
        return dic


class ModelAccState:
    link_no = 0
    timestamp = 0
    account = ''
    search = 0
    found = []

    @staticmethod
    def toDict(msg):
        dic = {}
        dic['link_no'] = msg.__getattribute__('link_no')
        dic['timestamp'] = msg.__getattribute__('timestamp')
        dic['account'] = msg.__getattribute__('account')
        dic['search'] = msg.__getattribute__('search')
        found = list(msg.__getattribute__('found'))
        ff = []
        for f in found:
            s = ModelUockValue.toDict(f)
            ff.append(s)
        dic['found'] = list(ff)
        return dic


class ModelUockValue:
    uock = 0
    value = 0
    height = 0
    vcn = 0

    @staticmethod
    def toDict(msg):
        dic = {}
        # dic['uock'] = msg.__getattribute__('uock')
        dic['uock'] = str(msg.__getattribute__('uock'))
        dic['value'] = str(msg.__getattribute__('value'))
        dic['height'] = msg.__getattribute__('height')
        dic['vcn'] = msg.__getattribute__('vcn')
        a = json.dumps(dic)
        return dic


class ModelUtxoState:
    link_no = 0
    heights = []
    indexes = []
    txns = []

    @staticmethod
    def toDict(msg):
        dic = {}
        dic['link_no'] = msg.__getattribute__('link_no')
        dic['heights'] = list(msg.__getattribute__('heights'))
        # dic['indexes'] = list(msg.__getattribute__('indexes'))
        _indexes = list(msg.__getattribute__('indexes'))
        indexs = []
        for index in _indexes:
            flag = index
            i = (flag >> 16) & 0xffff
            indexs.append(i)
        dic['indexes'] = indexs
        txns = list(msg.__getattribute__('txns'))
        tt = []
        for t in txns:
            s = ModelTxn.toDict(t)
            tt.append(s)
        dic['txns'] = list(tt)
        return dic


class ModelTxn:
    version = 0
    tx_in = []
    tx_out = []
    lock_time = 0
    sig_raw = ''

    @staticmethod
    def toDict(msg):
        dic = {}
        dic['version'] = msg.__getattribute__('version')
        dic['lock_time'] = msg.__getattribute__('lock_time')
        dic['sig_raw'] = msg.__getattribute__('sig_raw').hex()
        tx_ins = msg.__getattribute__('tx_in')
        tins = []
        for _in in tx_ins:
            s = ModelTxnIn.toDict(_in)
            tins.append(s)
        dic['tx_in'] = list(tins)
        tx_outs = msg.__getattribute__('tx_out')
        touts = []
        for _out in tx_outs:
            s = ModelTxnOut.toDict(_out)
            touts.append(s)
        dic['tx_out'] = list(touts)
        return dic


class ModelTxnIn:
    prev_output = ''
    sig_script = ''
    sequence = 0

    @staticmethod
    def toDict(msg):
        dic = {}
        dic['sig_script'] = msg.__getattribute__('sig_script').hex()
        dic['sequence'] = msg.__getattribute__('sequence')
        p = msg.__getattribute__('prev_output')
        dic['prev_output'] = ModelOutPoint.toDict(p)
        return dic


class ModelOutPoint:
    hash = ''
    index = 0

    @staticmethod
    def toDict(msg):
        dic = {}
        dic['hash'] = msg.__getattribute__('hash').hex()
        dic['index'] = msg.__getattribute__('index')
        return dic


class ModelTxnOut:
    value = 0
    pk_script = ''

    @staticmethod
    def toDict(msg):
        dic = {}
        dic['value'] = round(msg.__getattribute__('value') / 10**8, 8)
        pk = msg.__getattribute__('pk_script')
        tok = script.Tokenizer(pk)
        dic['pk_script'] = str(tok)
        return dic


# if __name__ == "__main__":
#     a = ModelBlockHeader()
#     a.link_no = 1
#     a.bits = 123456
#     s = ModelBlockHeader.toJson(a)
