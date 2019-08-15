import json
from flask import Flask, request
import re
import sys
import traceback
import random
import cgi
import urllib
import hashlib
import socket
import time
import os
import json

from wsgiref.simple_server import make_server
from binascii import hexlify, unhexlify

from nbc import util
from nbc import wallet
from nbc import coins
from nbc import protocol
from nbc import consensus

from nbc.dbconfig import config
from nbc.node import basenode
from nbc.blockchain import DbLocker

from model import *

if sys.version_info.major == 3:
    # urlparse.xx: urlencode, urljoin, urlparse, unquote, parse_qs, parse_qsl ...
    from urllib import parse as urlparse
    from urllib.request import urlopen
    make_long = int
else:
    import urlparse
    urlparse.urlencode = urllib.urlencode
    from urllib import urlopen
    make_long = long

__LocalDbg__ = True

global_router = None
curr_coin = coins.Newborntoken

_0 = b'\x00' * 32

# local startup by: python application.py -ENV apptype=RAW
config.load_env()
config.load_argv(
)  # sys.argv will overwrite os.environ  # -ENV name=value -CFG name=value

app_type_ = config.get('ENV/apptype', 'NODE')  # SYNC NODE RAW
cloud_db_ = config.get('ENV/clouddb', None)  # DYNAMODB
bind_vcn_ = int(config.get('CFG/vcn', '0'))  # should be current chain no
link_mask_ = int(config.get('CFG/mask', '0')) & 0xffff
cloud_factory_ = config.get('ENV/cloud')
cloud_readonly_ = int(config.get('ENV/cloudreadonly', 0))

local_datadir_ = os.environ.get('AWS_LOCAL_DIR', './database')

region_name = None
instance_id = ''

if cloud_factory_ == 'AWS':
    try:
        s = urlopen(
            'http://169.254.169.254/latest/meta-data/instance-id').read()
        if s:
            instance_id = s.decode('latin-1')
            print('find instance id:', instance_id)

        s = os.environ.get('AWS_REGION')
        if s:
            region_name = s
        else:
            s = urlopen(
                'http://169.254.169.254/latest/meta-data/placement/availability-zone'
            ).read()
            if s:
                region_name = s.decode('latin-1')
                if region_name[-1] in 'abcdefghijk':
                    region_name = region_name[:
                                              -1]  # cn-north-1a  --> cn-north-1
                os.environ['AWS_REGION'] = region_name
                print('find region name:', region_name)
    except:
        traceback.print_exc()

# ----------


def application(environ, start_response):
    handler = None
    groups = ()
    request_path = environ.get('PATH_INFO', '')
    for regexp, fn in global_router._url_mapping:
        match = regexp.match(request_path)
        if match:
            groups = match.groups()
            handler = fn
            break

    status = 200
    if handler:
        try:
            method = environ['REQUEST_METHOD']
            ret = handler(environ, method, start_response, *groups)
            if ret:
                return ret
            else:
                status = 404  # not processed, means resource inexistent
        except Exception as e:
            traceback.print_exc(file=sys.stdout)  # for debug
            # Error.error('%s: %s' % (e.__class__.__name__,str(e)))
            if __LocalDbg__:
                lines = ''.join(traceback.format_exception(*sys.exc_info()))
                start_response(Error.status_code_desc(500),
                               [('Content-Type', 'text/html; charset=utf-8')])
                return [('<pre>%s</pre>' %
                         cgi.escape(lines, quote=True)).encode('utf-8')]
            else:
                status = 500
    else:
        status = 404

    errDesc = Error.status_code_desc(status)
    sHtml = '<html><head><title>' + errDesc + \
        '</title></head><body><h3>' + errDesc + '</h3></body></html>'
    start_response(errDesc, [('Content-Type', 'text/html; charset=utf-8')])
    return [sHtml.encode('utf-8')]


class Error(Exception):
    __HTTP_STATUS_MESSAGES = {
        100: 'Continue',
        101: 'Switching Protocols',
        200: 'OK',
        201: 'Created',
        202: 'Accepted',
        203: 'Non-Authoritative Information',
        204: 'No Content',
        205: 'Reset Content',
        206: 'Partial Content',
        300: 'Multiple Choices',
        301: 'Moved Permanently',
        302: 'Moved Temporarily',
        303: 'See Other',
        304: 'Not Modified',
        305: 'Use Proxy',
        306: 'Unused',
        307: 'Temporary Redirect',
        400: 'Bad Request',
        401: 'Unauthorized',
        402: 'Payment Required',
        403: 'Forbidden',
        404: 'Not Found',
        405: 'Method Not Allowed',
        406: 'Not Acceptable',
        407: 'Proxy Authentication Required',
        408: 'Request Time-out',
        409: 'Conflict',
        410: 'Gone',
        411: 'Length Required',
        412: 'Precondition Failed',
        413: 'Request Entity Too Large',
        414: 'Request-URI Too Large',
        415: 'Unsupported Media Type',
        416: 'Requested Range Not Satisfiable',
        417: 'Expectation Failed',
        500: 'Internal Server Error',
        501: 'Not Implemented',
        502: 'Bad Gateway',
        503: 'Service Unavailable',
        504: 'Gateway Time-out',
        505: 'HTTP Version not supported'
    }

    @staticmethod
    def http_status(code):
        if code not in Error.__HTTP_STATUS_MESSAGES:
            raise Error('Invalid HTTP status code: %d' % code)
        return Error.__HTTP_STATUS_MESSAGES[code]

    @staticmethod
    def status_code_desc(code):
        return '%d %s' % (code, Error.http_status(code))

    @staticmethod
    def warning(s):
        print('!! warning:', s)

    @staticmethod
    def error(s):
        print('!! error:', s)


RE_FIND_GROUPS = re.compile('\(.*?\)')


class WSGIRouter:
    def __init__(self, handler_tuples):
        handler_map = {}
        pattern_map = {}
        url_mapping = []

        for regexp, handler in handler_tuples:
            handler_map[handler.__name__] = handler

            if not regexp.startswith('^'):
                regexp = '^' + regexp
            if not regexp.endswith('$'):
                regexp += '$'

            compiled = re.compile(regexp)
            url_mapping.append((compiled, handler))

            num_groups = len(RE_FIND_GROUPS.findall(regexp))
            handler_patterns = pattern_map.setdefault(
                handler, [])  # if already exist, reuse old one
            handler_patterns.append((compiled, num_groups))

        self._handler_map = handler_map  # {'admin/*/*':admin_process}
        # {admin:[(CompiledPatternRe,NumOfGroups),...]
        self._pattern_map = pattern_map
        # [(CompiledPatternRe,admin_process),...]
        self._url_mapping = url_mapping


# -----------


def binary_input(environ):
    data = environ['wsgi.input'].read(int(environ.get('CONTENT_LENGTH', 0)))
    return data, protocol.Message.parse(data, curr_coin.magic)  # data is bytes


def binary_result(code, binary, response,
                  headers=None):  # binary should be bytes
    headers2 = [('Content-Type', 'application/octet-stream'),
                ('Content-Length', str(len(binary)))]
    if headers:
        headers2.extend(headers)
    response(Error.status_code_desc(code), headers2)
    return [binary]


# def binay_result


def http_input(environ, is_qs=True):
    try:
        if is_qs:
            ret = {}
            query_str = environ.get('QUERY_STRING',
                                    '')  # request_arg is <class str>
            for (name, value) in urlparse.parse_qsl(
                    query_str):  # value of parameter has unquoted
                tmp = ret.get(name, None)
                if tmp is None:
                    ret[name] = value
                elif type(tmp) == list:
                    tmp.append(value)
                else:
                    ret[name] = [tmp, value]
            return ret
        else:
            body = environ['wsgi.input'].read(
                int(environ.get('CONTENT_LENGTH', 0)))
            return json.loads(body.decode('utf-8'))  # body is bytes
    except:
        traceback.print_exc()
        return {}


def http_result(code, d, response, headers=None):
    sRet = json.dumps(d).encode('utf-8')
    headers2 = [('Content-Type', 'application/json; charset=utf-8'),
                ('Content-Length', str(len(sRet)))]
    if headers:
        headers2.extend(headers)

    response(Error.status_code_desc(code), headers2)
    return [sRet]


str_type_ = type('')
bytes_type_ = type(b'')
list_type_ = [type([]), type(())]


def make_query_items_(bItem, sFmt):
    if sFmt[:1] == '[' and sFmt[-1:] == ']':
        return [make_query_items_(item, sFmt[1:-1]) for item in bItem]

    ret = []
    bFmt = sFmt.split(',')
    fmtLen = len(bFmt)
    for item in bItem:
        b = []
        for i in range(fmtLen):
            fmt = bFmt[i]
            v = item[i] if fmtLen > 1 else item
            if fmt == 'n':
                v = make_long(v)
            elif fmt == 'f':
                v = float(v)
            elif fmt == 's':
                if type(v) != str_type_:
                    v = str(v)
            elif fmt == 'b':
                if type(v) != bytes_type_:
                    v = v.encode('utf-8')
            # else, ignore converting, fmt can be 'x'
            b.append(v)

        if fmtLen == 1:
            ret.append(b[0])
        else:
            ret.append(b)

    return ret


# type prefix: n f b s x []
# such as: adjust_msg_format( param, [('vcn','n'),('pay_from','[n,b]'),
#   ('pay_to','[n,b]'),('scan_count','n'),('last_uocks','[n]')] )
def adjust_msg_format(param, fmt):
    try:
        for (attr, data_type
             ) in fmt:  # data_type can be any of 'nfsbx' or compose with '[]'
            if attr not in param:
                return False

            value = param[attr]
            if data_type == 'n':
                param[attr] = make_long(value)
            elif data_type == 'f':
                param[attr] = float(value)
            elif data_type == 'b':
                if type(value) != bytes_type_:
                    param[attr] = value.encode('utf-8')
            elif data_type == 's':
                if type(value) != str_type_:
                    param[attr] = str(value)
            elif data_type == 'x':  # keep no change
                pass
            elif data_type[:1] == '[' and data_type[-1:] == ']':
                if type(value) in list_type_:
                    param[attr] = make_query_items_(value, data_type[1:-1])
                else:
                    return False  # invalid value
            else:
                return False  # unknown data type

        return True
    except:
        traceback.print_exc()
        print('data format error:', param)

    return False


# ----------


def server_sign(sequence, pks_out, last_uocks, version, tx_in, tx_out,
                lock_time):
    # wait to do: sign by RAW or NODE wallet ...
    return b''


def root_(environ, method, response):
    if method == 'GET':
        # sHost = environ['HTTP_HOST']

        sHtml = b'OK'
        response('200 OK', [('Content-Type', 'text/html'),
                            ('Content-Length', str(len(sHtml)))])
        return [sHtml]

    return None


def mng_node_(environ, method, response, action):
    if action == 'echo':  # for testing
        if method == 'GET':
            param = http_input(environ)
            dRet = {'success': True, 'message': param.get('info', '')}
            return http_result(200, dRet, response)


def txn_sheets_(environ, method, response, action):
    if action == 'sheet':
        if method == 'POST':
            sErr = ''
            sequence = 0

            try:
                data, msg = binary_input(
                    environ)  # from post data with bitcoin-msg-format

                if msg.command == 'makesheet':
                    if (msg.vcn & node._link_mask) != node._chain_no:
                        sErr = 'chain no mismatch'

                    elif DbLocker.wait_enter(15):  # max wait 10 seconds
                        ret = None
                        try:
                            if node.in_catching():
                                sErr = 'node not ready'
                            else:
                                sequence = msg.sequence
                                pay_from = [(item.value, item.address)
                                            for item in msg.pay_from]
                                pay_to = [(item.value, item.address)
                                          for item in msg.pay_to]
                                (tx, pks_list,
                                 last_uocks) = node.make_txn_sheet(
                                     pay_from, pay_to, msg.scan_count,
                                     msg.min_utxo, msg.max_utxo, msg.sort_flag,
                                     msg.last_uocks)
                                if tx:
                                    b = [
                                        protocol.format.VarStrList(item)
                                        for item in pks_list
                                    ]
                                    sig = server_sign(msg.sequence, b,
                                                      last_uocks, tx.version,
                                                      tx.tx_in, tx.tx_out,
                                                      tx.lock_time)
                                    msg2 = protocol.OrgSheet(
                                        msg.sequence, b, last_uocks,
                                        tx.version, tx.tx_in, tx.tx_out,
                                        tx.lock_time, sig)
                                    ret = binary_result(
                                        200, msg2.binary(node.coin.magic),
                                        response)
                        except Exception as e:
                            sErr = str(e)
                            traceback.print_exc()
                        DbLocker.leave()
                        if ret:
                            return ret
                    else:
                        sErr = 'require DB locker failed'
                else:
                    sErr = 'node in busy'
            except Exception as e:
                sErr = str(e)
                traceback.print_exc()

            if not sErr:
                sErr = 'unknown error'
            bin_msg = protocol.UdpReject(sequence, sErr,
                                         'makesheet').binary(node.coin.magic)
            return binary_result(200, bin_msg, response)

    elif action == 'txn':
        if method == 'POST':
            sErr = ''

            try:
                data, msg = binary_input(environ)

                # wait to do: set msg.sig_raw and adjust data
                # ...

                if msg.command != 'tx':
                    sErr = 'invalid txn format'

                elif DbLocker.wait_enter(15):
                    ret = None
                    try:
                        if node.in_catching():
                            sErr = 'node not ready'
                        else:
                            node._apiserver_cmd.insert(
                                0, ('relay_sheet', msg, data))
                            hash_ = util.sha256d(
                                data[24:-1]
                            )  # prefix message is 24 bytes, last byte is sig_raw, the left is payload
                            msg2 = protocol.UdpConfirm(hash_, 0xffffffff)
                            ret = binary_result(200,
                                                msg2.binary(node.coin.magic),
                                                response)
                    except Exception as e:
                        sErr = str(e)
                        traceback.print_exc()
                    DbLocker.leave()
                    if ret:
                        return ret
                else:
                    sErr = 'require DB locker failed'
            except Exception as e:
                sErr = str(e)
                traceback.print_exc()

            if not sErr:
                sErr = 'unknown error'
            bin_msg = protocol.UdpReject(0, sErr,
                                         'transaction').binary(node.coin.magic)
            return binary_result(200, bin_msg, response)

    elif action == 'state':
        if method == 'GET':
            sErr = ''

            try:
                param = http_input(environ)
                txn_hash = param.get('hash',
                                     '')  # txn_hash should be utf-8 string
                txn_detail = int(param.get('detail', 0))

                if txn_hash:
                    txn_hash = unhexlify(
                        txn_hash)  # txn_hash has changed to bytes

                    if DbLocker.wait_enter(15):
                        ret = None
                        try:
                            if node.in_catching():
                                sErr = 'node not ready'
                            else:
                                txn2 = node.txns_get(
                                    txn_hash, only_main=True
                                )  # get mainchain transcation
                                if txn2:
                                    block = node.block_get_(txn2)
                                    if block and block.mainchain:
                                        if txn_detail:
                                            bin_msg = protocol.UtxoState(
                                                node._link_no, [block.height],
                                                [txn2.index << 16], [txn2])
                                            ret = binary_result(
                                                200,
                                                bin_msg.binary(
                                                    node.coin.magic), response)
                                        else:
                                            confirm_num = node.chain_height - block.height
                                            txn_idx = txn2.index
                                            msg2 = protocol.UdpConfirm(
                                                txn_hash,
                                                ((txn_idx & 0xffff) << 48) |
                                                ((confirm_num & 0xffff) << 32)
                                                | (block.height & 0xffffffff))
                                            ret = binary_result(
                                                200,
                                                msg2.binary(node.coin.magic),
                                                response)
                                    else:
                                        sErr = 'invalid block'
                                else:
                                    txn2 = node._search_mempool(txn_hash)
                                    if txn2:
                                        if txn_detail:
                                            bin_msg = protocol.UtxoState(
                                                node._link_no, [0], [0],
                                                [txn2])
                                            ret = binary_result(
                                                200,
                                                bin_msg.binary(
                                                    node.coin.magic), response)
                                        else:
                                            sErr = 'in pending state'
                                    else:
                                        sErr = 'no transaction'
                        except Exception as e:
                            sErr = str(e)
                            traceback.print_exc()
                        DbLocker.leave()
                        if ret:
                            return ret
                    else:
                        sErr = 'require DB locker failed'
                else:
                    sErr = 'invalid argument'
            except Exception as e:
                sErr = str(e)
                traceback.print_exc()

            if not sErr:
                sErr = 'unknown error'
            bin_msg = protocol.UdpReject(0, sErr,
                                         'state').binary(node.coin.magic)
            return binary_result(200, bin_msg, response)

    elif action == 'txns':  # receive many txn, it is useful for testing
        if method == 'POST':
            succNum = 0
            errNum = 0
            try:
                dataBuf = environ['wsgi.input'].read(
                    int(environ.get('CONTENT_LENGTH', 0)))

                while True:
                    msgLen = protocol.Message.first_msg_len(dataBuf)
                    if msgLen is None or msgLen > len(dataBuf):
                        break  # ignore if not enough bytes

                    # parse the message and handle it
                    data = dataBuf[:msgLen]
                    dataBuf = dataBuf[msgLen:]
                    msg = protocol.Message.parse(data, curr_coin.magic)

                    # wait to do: set msg.sig_raw and adjust data
                    # ...

                    if msg.command == 'tx' and DbLocker.wait_enter(15):
                        try:
                            if node.in_catching():
                                print(
                                    'tx command received, but node not ready')
                            else:
                                node._apiserver_cmd.insert(
                                    0, ('relay_sheet', msg, data))
                                # not reply: protocol.UdpConfirm(util.sha256d(data[24:-1]),0xffffffff)
                                succNum += 1
                        except Exception as e:
                            errNum += 1
                            traceback.print_exc()
                        DbLocker.leave()
            except Exception as e:
                traceback.print_exc()

            sErr = '%i:%i' % (succNum, errNum)
            bin_msg = protocol.UdpReject(0, sErr,
                                         'txns').binary(node.coin.magic)
            return binary_result(200, bin_msg, response)


def txn_state_(environ, method, response, action):
    if action == 'account':
        if method == 'GET':
            sErr = ''

            try:
                param = http_input(environ)
                account = param.get('addr', '')  # addr should be utf-8 string
                uock = int(param.get('uock', '0'))  # uock from
                uock2 = int(param.get('uock2', '0'))  # uock before

                if account:
                    if DbLocker.wait_enter(15):
                        ret = None
                        try:
                            if node.in_catching():
                                sErr = 'node not ready'
                            else:
                                addr2 = util.base58.decode_check(
                                    account
                                )  # the input of decode_check can be str or bytes
                                coin_hash = addr2[
                                    3:]  # coin_hash: pub_hash32 + cointype
                                utxos = node.list_unspent(
                                    coin_hash, 1024, 0, 0, uock,
                                    uock2)  # limit = 1024

                                bOut = [
                                    protocol.format.UockValue(
                                        u.uock, u.value, u.height, u.vcn)
                                    for u in utxos
                                ]
                                bin_msg = protocol.AccState(
                                    node._link_no, int(time.time()), account,
                                    1024, bOut)
                                ret = binary_result(
                                    200, bin_msg.binary(node.coin.magic),
                                    response)
                        except Exception as e:
                            sErr = str(e)
                            traceback.print_exc()
                        DbLocker.leave()
                        if ret:
                            return ret
                    else:
                        sErr = 'require DB locker failed'
                else:
                    sErr = 'invalid parameter'
            except Exception as e:
                sErr = str(e)
                traceback.print_exc()

            if not sErr:
                sErr = 'unknown error'
            bin_msg = protocol.UdpReject(0, sErr,
                                         'utxos').binary(node.coin.magic)
            return binary_result(200, bin_msg, response)

    elif action == 'block':
        if method == 'GET':
            sErr = ''
            try:
                param = http_input(environ)
                bk_hash = param.get('hash', '')
                if not bk_hash:
                    bk_hash = _0
                else:
                    bk_hash = unhexlify(bk_hash)
                heights = param.get('hi', [])
                if type(heights) != list:
                    heights = [heights]
                heights = [int(h) for h in heights]

                if DbLocker.wait_enter(15):
                    ret = None
                    try:
                        if node.in_catching():
                            sErr = 'node not ready'
                        else:
                            blocks = []
                            exists = []
                            highest = node.chain_height
                            if bk_hash != _0:
                                bk = node.block_get3(bk_hash)
                                if bk:
                                    blocks.append(bk)
                                    exists.append(bk.height)
                            for hi in heights:
                                if hi < 0:
                                    hi = highest + hi + 1
                                if hi >= 0 and (hi not in exists):
                                    bk = node.block_get2(hi)
                                    if bk:
                                        blocks.append(bk)
                                        exists.append(bk.height)

                            bb = []
                            hh = []
                            tt = []
                            for bk in blocks:
                                bb.append(protocol.BlockHeader.from_block(bk))
                                hh.append(bk.height)
                                tt.append(bk._blockid << 20)

                            bin_msg = protocol.ReplyHeaders(
                                node._link_no, hh, tt, bb)
                            ret = binary_result(
                                200, bin_msg.binary(node.coin.magic), response)
                    except Exception as e:
                        sErr = str(e)
                        traceback.print_exc()
                    DbLocker.leave()
                    if ret:
                        return ret
                else:
                    sErr = 'require DB locker failed'
            except Exception as e:
                sErr = str(e)
                traceback.print_exc()

            if not sErr:
                sErr = 'unknown error'
            bin_msg = protocol.UdpReject(0, sErr,
                                         'blocks').binary(node.coin.magic)
            return binary_result(200, bin_msg, response)

    elif action == 'uock':
        if method == 'GET':
            sErr = ''

            try:
                param = http_input(environ)
                account = param.get('addr', '')  # addr should be utf-8 string
                num = min(int(param.get('num', '0')),
                          1024)  # max get 1024 record
                uocks = param.get('uock', [])
                if type(uocks) != list:
                    uocks = [uocks]
                uocks = [int(u, 16) for u in uocks]

                if uocks or (num and account):
                    if DbLocker.wait_enter(15):
                        ret = None
                        try:
                            if node.in_catching():
                                sErr = 'node not ready'
                            else:
                                if uocks:
                                    utxos = node.list_unspent2(
                                        uocks
                                    )  # owner txn of every uock has been cached
                                else:
                                    addr2 = util.base58.decode_check(
                                        account
                                    )  # the input of decode_check can be str or bytes
                                    coin_hash = addr2[
                                        3:]  # coin_hash: pub_hash32 + cointype
                                    utxos = node.list_unspent(
                                        coin_hash, num, 0, 0, -1
                                    )  # limit = num, uock=-1 means get from lastest one, owner txn cached

                                hi = []
                                idx = []
                                txns = []
                                for u in utxos:
                                    hi.append(u.height)
                                    idx.append(((
                                        (u.uock >> 20) & 0xffff) << 16)
                                        | (u.uock & 0xffff))
                                    txns.append(u.get_txn())

                                bin_msg = protocol.UtxoState(
                                    node._link_no, hi, idx, txns)
                                ret = binary_result(
                                    200, bin_msg.binary(node.coin.magic),
                                    response)
                        except Exception as e:
                            sErr = str(e)
                            traceback.print_exc()
                        DbLocker.leave()
                        if ret:
                            return ret
                    else:
                        sErr = 'require DB locker failed'
                else:
                    sErr = 'invalid parameter'
            except Exception as e:
                sErr = str(e)
                traceback.print_exc()

            if not sErr:
                sErr = 'unknown error'
            bin_msg = protocol.UdpReject(0, sErr,
                                         'txns').binary(node.coin.magic)
            return binary_result(200, bin_msg, response)


# -----

re_newline_ = re.compile(r'\r\n|\n|\r')

_dbg_ver = [0, 0, 2, 1]
_dbg_secret = os.environ.get('AWS_DBG_SECRET', '')
_dbg_nonce = 0
_dbg_query_at = 0  # temporary using between query_login and get_login

_dbg_start_at = 0
_dbg_session = ''

DBG_PUBKEY = b'\x03\xbd\xfaa\x92\x9d\xf7\x1bj\xed\xb7\x88\x8f.\xe7\xd3c\xbb\xc7\x97\xe4\xe0i\x13[\x94\x80\t\x90\xd6I\x9a\xac'


def testing2_(environ, method, response, action):
    global _dbg_nonce, _dbg_query_at, _dbg_start_at, _dbg_session
    if not __LocalDbg__:
        return None

    if action == 'debug':  # for testing
        if method == 'POST':
            param = http_input(environ)
            if _dbg_session != param.get('sid', '') or (
                    time.time() - _dbg_start_at) > 3600:  # 3600 is 1 hour
                response('401 Unauthorized', [])
                return [b'']

            data = environ['wsgi.input'].read(
                int(environ.get('CONTENT_LENGTH', 0)))  # data is bytes
            lines = re_newline_.split(data.decode('utf-8'))

            isExec = False
            if len(lines) > 1:
                isExec = True
                lines = '\n'.join(lines)
                print('debug> exec multiple lines')

            else:  # only one line, 'exec' or 'eval'
                if not lines[0]:
                    response('200 OK', [])
                    return [b'']

                lines = lines[0]
                if lines == 'exit()':
                    response('200 OK', [])
                    return [b'disable run exit()']

                print('debug>', lines)
                try:
                    compile(lines, 'stdin', 'eval')
                except SyntaxError:
                    isExec = True

            ret = None
            if isExec:
                try:
                    exec(lines, globals())
                except Exception as e:
                    ret = str(e)
                    traceback.print_exc()
            else:
                try:
                    ret = str(eval(lines, globals()))
                except Exception as e:
                    ret = str(e)
                    traceback.print_exc()

            if type(ret) != str:
                ret = ''
            response('200 OK', [('Content-Type', 'text/plain; charset=utf-8')])
            return [ret.encode('utf-8')]

    elif action == 'query_login':
        if method == 'GET':
            _dbg_nonce = (random.randint(0, 65535) << 16) + random.randint(
                0, 65535)
            _dbg_query_at = int(time.time())
            response('200 OK', [('Content-Type', 'text/plain; charset=utf-8')])
            return [b'%i,%08x' % (_dbg_query_at, _dbg_nonce)]

    elif action == 'get_login':
        if method == 'POST':
            if int(time.time()
                   ) - _dbg_query_at > 120:  # should login within 120 seconds
                response('401 Unauthorized', [])
                return [b'']

            sh = hashlib.sha1(
                ('%s#%i#%08x' % (_dbg_secret, _dbg_query_at,
                                 _dbg_nonce)).encode('utf-8')).hexdigest()

            data = environ['wsgi.input'].read(
                int(environ.get('CONTENT_LENGTH', 0)))  # data is bytes
            data = json.loads(data.decode('utf-8'))
            if data and sh == data.get('session'):
                sig = unhexlify(data.get('signature', ''))
                wa = wallet.Address(pub_key=DBG_PUBKEY,
                                    vcn=0,
                                    coin_type=b'\x00')
                if wa.verify(unhexlify(sh), sig):
                    _dbg_session = sh
                    _dbg_start_at = _dbg_query_at
                    response('200 OK',
                             [('Content-Type', 'text/plain; charset=utf-8')])
                    return [sh.encode('utf-8')]

            response('401 Unauthorized', [])
            return [b'']

    elif action == 'set_patch':
        if method == 'POST':
            param = http_input(environ)
            if _dbg_session != param.get('sid', '') or (
                    time.time() - _dbg_start_at) > 3600:  # 3600 is 1 hour
                response('401 Unauthorized', [])
                return [b'']

            data = environ['wsgi.input'].read(
                int(environ.get('CONTENT_LENGTH', 0)))  # data is bytes
            data = data.decode('utf-8')
            with open(os.path.join(local_datadir_, 'patch.py'), 'wt') as f:
                f.write(data)

            response('200 OK', [])
            return [b'%i' % (len(data), )]


def testing_(environ, method, response, action):
    return testing2_(environ, method, response, action)  # support redefine


global_router = WSGIRouter([
    ('/', root_),
    ('/mng/node/(.*?)', mng_node_),
    ('/txn/sheets/(.*?)', txn_sheets_),
    ('/txn/state/(.*?)', txn_state_),
    ('/testing/(.*?)', testing_),
])

# ----------
POET_PUBKEY = b'\x03\xdeC%\xe7HO\x8b\xcb\xcb/\x85M\x93\xc9/\xda\x0bm\xbe\xbc\xe7]X\x06|\xeaP\xfb\xe5[\xfd\t'
consensus.init_tee(
    wallet.Address(pub_key=POET_PUBKEY, vcn=0, coin_type=b'\x00'))

curr_link_ = bind_vcn_ & link_mask_  # total link number is: _link_mask + 1
link_no_ = (link_mask_ << 16) | curr_link_

instance_ = None
db_backup = None


def ec2_reboot():
    if instance_:
        instance_.reboot()


def check_run_patch():
    sFile = os.path.join(local_datadir_,
                         'patch.py')  # not depends __LocalDbg__
    if os.path.isfile(sFile):

        with open(sFile, 'rt') as f:
            sCode = f.read()

            targVer = '.'.join(str(ch) for ch in _dbg_ver)
            m = re.match(r'^#\W*([.0-9]+)', sCode)
            if m and m.group(1) == targVer:
                print('start patch code for ver %s' % targVer)
                try:
                    exec(sCode, globals())
                except:
                    traceback.print_exc()


def exit_node(
):  # exit process but no call ec2_reboot()  # exit all threads except MainThread
    if __LocalDbg__:
        from nbc.node import mining

        # step 1: try safe close, avoid reboot
        if db_backup:
            db_backup.ec2_reboot = None
        node.close()

        # step 2: try again if node not success close
        time.sleep(5)
        if not node._exited:
            node.handle_close()

        # step 3: wait exiting
        succ = False
        for i in range(300):  # max wait 300 seconds
            if (not db_backup or not db_backup._active) and (
                    not mining._poet_thread
                    or not mining._poet_thread._active):
                succ = True
                break
            time.sleep(1)

        return succ


def reboot_app():
    if __LocalDbg__ and exit_node():
        ec2_reboot()
        return True
    else:
        return False


if app_type_ == 'RAW':
    from nbc.node import raw
    if cloud_db_ == 'DYNAMODB':
        node = raw.RawNode(data_dir=local_datadir_,
                           address=('0.0.0.0', 20303),
                           link_no=link_no_,
                           coin=curr_coin)
        node.log_level = node.LOG_LEVEL_DEBUG  # LOG_LEVEL_DEBUG LOG_LEVEL_INFO

        check_run_patch()
        basenode.startServer(node)
    else:
        print('fatal error: environ-var (ENV_clouddb=DYNAMODB) is required')

else:  # 'NODE' or 'SYNC'
    if cloud_db_ == 'DYNAMODB':
        from nbc.blockchain import database as _database
        db_backup = _database.db_backup
        if cloud_readonly_:
            db_backup.readonly = True
        else:
            db_backup.ec2_reboot = ec2_reboot
    else:
        db_backup = None  # local DB not backup to cloud-db

        # quick recover from zip if all defined: AWS_LOCAL_DIR,AWS_DB_BUCKET,AWS_DB_PATH
        s3db_bucket = os.environ.get('AWS_DB_BUCKET', '')
        s3db_path = os.environ.get('AWS_DB_PATH', '')
        if local_datadir_ and s3db_bucket and s3db_path:
            db_backup = basenode.ZipBackup(os.path.join(local_datadir_, 'zip'),
                                           s3db_bucket, s3db_path)
            # db_backup.readonly = True

    def s3_upload_db(s3alias):  # such as 'db-user1-node-190616'
        if not __LocalDbg__:
            return 0
        if not db_backup:
            return 0
        if not exit_node():
            return 0
        return db_backup.s3_upload_db(local_datadir_, s3alias)

    def s3_download_db(s3alias):  # such as 'db-user1-node-190616'
        if not __LocalDbg__:
            return 0
        if not db_backup:
            return 0
        if not exit_node():
            return 0
        return db_backup.s3_download_db(local_datadir_, s3alias)

    if cloud_factory_ == 'AWS':
        import boto3

        port_succ = False
        if region_name and instance_id:
            try:
                ec2 = boto3.resource('ec2', region_name=region_name)
                instance_ = ec2.Instance(instance_id)
                secur_groups = instance_.security_groups

                if secur_groups:
                    exist30303 = False
                    exist30302 = False
                    secur_group = ec2.SecurityGroup(secur_groups[0]['GroupId'])
                    permis = secur_group.ip_permissions
                    for item in permis:
                        if item['FromPort'] == 30303:
                            exist30303 = item['IpProtocol'] == 'tcp'
                        elif item['FromPort'] == 30302:
                            exist30302 = item['IpProtocol'] == 'udp'

                    if not exist30303 or not exist30302:
                        permit = []
                        if not exist30303:
                            permit.append({
                                'IpRanges': [{
                                    'CidrIp': '0.0.0.0/0'
                                }],
                                'FromPort': 30303,
                                'IpProtocol': 'tcp',
                                'ToPort': 30303
                            })
                        if not exist30302:
                            permit.append({
                                'IpRanges': [{
                                    'CidrIp': '0.0.0.0/0'
                                }],
                                'FromPort': 30302,
                                'IpProtocol': 'udp',
                                'ToPort': 30302
                            })
                        secur_group.authorize_ingress(IpPermissions=permit)
                        print('security group permission (30302/30303) added.')
                    # else: both port already openned
                    port_succ = True
            except:
                traceback.print_exc()

        if not port_succ:
            print('warning: open 30303 or 30302 port failed.')

    node = basenode.SyncNode(data_dir=local_datadir_,
                             address=('0.0.0.0', 30303),
                             link_no=link_no_,
                             coin=curr_coin)
    # LOG_LEVEL_DEBUG LOG_LEVEL_INFO  # default is LOG_LEVEL_ERROR
    node.log_level = node.LOG_LEVEL_INFO

    check_run_patch()
    basenode.startServer(node, db_backup)
    if app_type_ == 'NODE':  # not only sync, mining also
        node._mine_pool.start()
# else, app_type is 'SYNC', no mining

# print('Server starting...')

# if __name__ == '__main__':
#   httpd = make_server('', 8080, application)
#   httpd.serve_forever()


if sys.version_info.major == 3:
    # urlparse.xx: urlencode, urljoin, urlparse, unquote, parse_qs, parse_qsl ...
    from urllib import parse as urlparse
    from urllib.request import urlopen
    make_long = int
else:
    import urlparse
    urlparse.urlencode = urllib.urlencode
    from urllib import urlopen
    make_long = long

__LocalDbg__ = True

global_router = None
curr_coin = coins.Newborntoken

_0 = b'\x00' * 32

# local startup by: python application.py -ENV apptype=RAW
config.load_env()
config.load_argv(
)  # sys.argv will overwrite os.environ  # -ENV name=value -CFG name=value

app_type_ = config.get('ENV/apptype', 'NODE')  # SYNC NODE RAW
cloud_db_ = config.get('ENV/clouddb', None)  # DYNAMODB
bind_vcn_ = int(config.get('CFG/vcn', '0'))  # should be current chain no
link_mask_ = int(config.get('CFG/mask', '0')) & 0xffff
cloud_factory_ = config.get('ENV/cloud')
cloud_readonly_ = int(config.get('ENV/cloudreadonly', 0))

local_datadir_ = os.environ.get('AWS_LOCAL_DIR', './database')

region_name = None
instance_id = ''

if cloud_factory_ == 'AWS':
    try:
        s = urlopen(
            'http://169.254.169.254/latest/meta-data/instance-id').read()
        if s:
            instance_id = s.decode('latin-1')
            print('find instance id:', instance_id)

        s = os.environ.get('AWS_REGION')
        if s:
            region_name = s
        else:
            s = urlopen(
                'http://169.254.169.254/latest/meta-data/placement/availability-zone'
            ).read()
            if s:
                region_name = s.decode('latin-1')
                if region_name[-1] in 'abcdefghijk':
                    region_name = region_name[:
                                              -1]  # cn-north-1a  --> cn-north-1
                os.environ['AWS_REGION'] = region_name
                print('find region name:', region_name)
    except:
        traceback.print_exc()

app = Flask(__name__)


def after_request(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'PUT,GET,POST,DELETE'
    response.headers[
        'Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
    return response


@app.route('/account', methods=['POST'])
def account():
    sErr = ''
    try:
        account = request.form.get('account')
        print('>>> post account:', account)
        # account = param.get('addr','')  # addr should be utf-8 string
        # todo uock
        uock = 0
        uock2 = 0
        # uock = int(param.get('uock', '0'))  # uock from
        # uock2 = int(param.get('uock2', '0'))  # uock before

        if account:
            if DbLocker.wait_enter(15):
                ret = None
                try:
                    if node.in_catching():
                        sErr = 'node not ready'
                    else:
                        addr2 = util.base58.decode_check(
                            account
                        )  # the input of decode_check can be str or bytes
                        coin_hash = addr2[
                            3:]  # coin_hash: pub_hash32 + cointype
                        utxos = node.list_unspent(coin_hash, 1024, 0, 0, uock,
                                                  uock2)  # limit = 1024

                        bOut = [
                            protocol.format.UockValue(u.uock, u.value,
                                                      u.height, u.vcn)
                            for u in utxos
                        ]
                        bin_msg = protocol.AccState(node._link_no,
                                                    int(time.time()), account,
                                                    1024, bOut)
                        dic = ModelAccState.toDict(bin_msg)
                        # ret = binary_result(200,
                        #                     bin_msg.binary(node.coin.magic),
                        #                     response)
                        jsonStr = json.dumps(dic)
                        print('>>> jsonStr:', jsonStr)
                        return jsonStr
                except Exception as e:
                    sErr = str(e)
                    traceback.print_exc()
                DbLocker.leave()
                if ret:
                    return ret
            else:
                sErr = 'require DB locker failed'
        else:
            sErr = 'invalid parameter'
    except Exception as e:
        sErr = str(e)
        traceback.print_exc()

    # if not sErr: sErr = 'unknown error'
    # bin_msg = protocol.UdpReject(0,sErr,'utxos').binary(node.coin.magic)
    # return binary_result(200,bin_msg,response)
    return 'error'


def account1(account='1118hfRMRrJMgSCoV9ztyPcjcgcMZ1zThvqRDLUw3xCYkZwwTAbJ5o'):
    sErr = ''
    try:
        # account = request.form.get('account')
        print('>>> post account:', account)
        # account = param.get('addr','')  # addr should be utf-8 string
        # todo uock
        uock = 0
        uock2 = 0
        # uock = int(param.get('uock', '0'))  # uock from
        # uock2 = int(param.get('uock2', '0'))  # uock before

        if account:
            if DbLocker.wait_enter(15):
                ret = None
                try:
                    if node.in_catching():
                        sErr = 'node not ready'
                    else:
                        addr2 = util.base58.decode_check(
                            account
                        )  # the input of decode_check can be str or bytes
                        coin_hash = addr2[
                            3:]  # coin_hash: pub_hash32 + cointype
                        utxos = node.list_unspent(coin_hash, 1024, 0, 0, uock,
                                                  uock2)  # limit = 1024

                        bOut = [
                            protocol.format.UockValue(u.uock, u.value,
                                                      u.height, u.vcn)
                            for u in utxos
                        ]
                        bin_msg = protocol.AccState(node._link_no,
                                                    int(time.time()), account,
                                                    1024, bOut)
                        dic = ModelAccState.toDict(bin_msg)
                        jsonStr = json.dumps(dic)
                        print(jsonStr)
                        return jsonStr
                except Exception as e:
                    sErr = str(e)
                    traceback.print_exc()
                DbLocker.leave()
                if ret:
                    return ret
            else:
                sErr = 'require DB locker failed'
        else:
            sErr = 'invalid parameter'
    except Exception as e:
        sErr = str(e)
        traceback.print_exc()

    # if not sErr: sErr = 'unknown error'
    # bin_msg = protocol.UdpReject(0,sErr,'utxos').binary(node.coin.magic)
    # return binary_result(200,bin_msg,response)
    return sErr


@app.route('/')
def hello():
    return 'hello'

@app.route('/get_block', methods=['POST'])
def get_block():
    # hi=None
    print(">>>>>>>>>>>>>>>get_block<<<<<<<<<<<<<<<<<")
    hi = request.form.get('hi')
    print('>>> post hi:', hi)
    if hi == None:
        # default items
        heights = [-1, -2, -3, -4, -5, -6, -7, -8, -9, -10]
    else:
        c = hi.split(",")
        heights = c
        if type(heights) != list:
            heights = [heights]
        heights = [int(h) for h in heights]
    bk_hash = None
    if not bk_hash:
        bk_hash = _0
    else:
        bk_hash = unhexlify(bk_hash)

    if DbLocker.wait_enter(15):
        ret = None
        try:
            if node.in_catching():
                sErr = 'node not ready'
            else:
                blocks = []
                exists = []
                highest = node.chain_height
                if bk_hash != _0:
                    bk = node.block_get3(bk_hash)
                    if bk:
                        blocks.append(bk)
                        exists.append(bk.height)
                print('>>> heights:', heights)
                for hi in heights:
                    if hi < 0:
                        hi = highest + hi + 1
                    if hi >= 0 and (hi not in exists):
                        bk = node.block_get2(hi)
                        if bk:
                            blocks.append(bk)
                            exists.append(bk.height)

                bb = []
                hh = []
                tt = []
                for bk in blocks:
                    bb.append(protocol.BlockHeader.from_block(bk))
                    hh.append(bk.height)
                    tt.append(bk._blockid << 20)
                bin_msg = protocol.ReplyHeaders(node.link_no, hh, tt, bb)
                # tojson
                dic = ModelReplyHeaders.toDict(bin_msg)
                jsonStr = json.dumps(dic)
                print('>>> jsonStr:', jsonStr)
                return jsonStr
        except Exception as e:
            sErr = str(e)
            traceback.print_exc()
        DbLocker.leave()
        if not sErr:
            sErr = 'unknown error'
        bin_msg = protocol.UdpReject(0, sErr, 'blocks').binary(node.coin.magic)
        # return binary_result(200,bin_msg,response)
        return 'error'
    # return "hello"


# app.route('/get_uock',methods=['POST'])
@app.route('/get_uock', methods=['POST'])
def get_uock():
    sErr = ''
    try:
        uock = request.form.get('uock')
        print('>>> post uocks:', uock)
        uock = int(uock)
        account = '1118hfRMRrJMgSCoV9ztyPcjcgcMZ1zThvqRDLUw3xCYkZwwTAbJ5o'
        uocks = [uock]
        # param = http_input(environ)
        # account = param.get('addr', '')  # addr should be utf-8 string
        # num = min(int(param.get('num', '0')), 1024)  # max get 1024 record
        # uocks = param.get('uock', [])
        # if type(uocks) != list: uocks = [uocks]
        # uocks = [int(u, 16) for u in uocks]

        if uocks or (num and account):
            if DbLocker.wait_enter(15):
                ret = None
                try:
                    if node.in_catching():
                        sErr = 'node not ready'
                    else:
                        if uocks:
                            utxos = node.list_unspent2(
                                uocks
                            )  # owner txn of every uock has been cached
                        else:
                            addr2 = util.base58.decode_check(
                                account
                            )  # the input of decode_check can be str or bytes
                            coin_hash = addr2[
                                3:]  # coin_hash: pub_hash32 + cointype
                            utxos = node.list_unspent(
                                coin_hash, num, 0, 0, -1
                            )  # limit = num, uock=-1 means get from lastest one, owner txn cached

                        hi = []
                        idx = []
                        txns = []
                        for u in utxos:
                            hi.append(u.height)
                            idx.append((((u.uock >> 20) & 0xffff) << 16)
                                       | (u.uock & 0xffff))
                            txns.append(u.get_txn())

                        bin_msg = protocol.UtxoState(node._link_no, hi, idx,
                                                     txns)
                        dic = ModelUtxoState.toDict(bin_msg)

                        print('>>> bin_msg:', bin_msg)
                        # print('>>> dic:', dic)
                        jsonStr = json.dumps(dic)
                        print('>>> jsonStr:', jsonStr)
                        return jsonStr
                        # ret = binary_result(
                        #     200, bin_msg.binary(node.coin.magic), response)
                except Exception as e:
                    sErr = str(e)
                    traceback.print_exc()
                DbLocker.leave()
                if ret:
                    return ret
            else:
                sErr = 'require DB locker failed'
        else:
            sErr = 'invalid parameter'
    except Exception as e:
        sErr = str(e)
        traceback.print_exc()

    if not sErr:
        sErr = 'unknown error'
    bin_msg = protocol.UdpReject(0, sErr, 'txns').binary(node.coin.magic)
    # return binary_result(200, bin_msg, response)
    return 'error'


if __name__ == "__main__":
    print('Server starting...')
    # get_block()
    # account1()
    # get_uock()
    app.after_request(after_request)
    app.run(host='0.0.0.0', port=3001, debug=True)
