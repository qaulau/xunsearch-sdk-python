# -*- encoding: utf-8 -*-
#

"""迅搜(xunsearch) Python SDK封装

Python version of xunsearchd client (Python API)
"""

__author__ = 'qaulau'

import os
import re
import sys
import math
import select
import hashlib
from struct import pack, unpack
from collections import namedtuple, OrderedDict
import socket


CMD_NONE = 0
CMD_DEFAULT = CMD_NONE
CMD_PROTOCOL = 20110707
CMD_USE = 1
CMD_HELLO = 1
CMD_DEBUG = 2
CMD_TIMEOUT = 3
CMD_QUIT = 4
CMD_INDEX_SET_DB = 32
CMD_INDEX_GET_DB = 33
CMD_INDEX_SUBMIT = 34
CMD_INDEX_REMOVE = 35
CMD_INDEX_EXDATA = 36
CMD_INDEX_CLEAN_DB = 37
CMD_DELETE_PROJECT = 38
CMD_INDEX_COMMIT = 39
CMD_INDEX_REBUILD = 40
CMD_FLUSH_LOGGING = 41
CMD_INDEX_SYNONYMS = 42
CMD_INDEX_USER_DICT = 43
CMD_SEARCH_DB_TOTAL = 64
CMD_SEARCH_GET_TOTAL = 65
CMD_SEARCH_GET_RESULT = 66
CMD_SEARCH_SET_DB = CMD_INDEX_SET_DB
CMD_SEARCH_GET_DB = CMD_INDEX_GET_DB
CMD_SEARCH_ADD_DB = 68
CMD_SEARCH_FINISH = 69
CMD_SEARCH_DRAW_TPOOL = 70
CMD_SEARCH_ADD_LOG = 71
CMD_SEARCH_GET_SYNONYMS = 72
CMD_SEARCH_SCWS_GET = 73
CMD_QUERY_GET_STRING = 96
CMD_QUERY_GET_TERMS = 97
CMD_QUERY_GET_CORRECTED = 98
CMD_QUERY_GET_EXPANDED = 99
CMD_OK = 128
CMD_ERR = 129
CMD_SEARCH_RESULT_DOC = 140
CMD_SEARCH_RESULT_FIELD = 141
CMD_SEARCH_RESULT_FACETS = 142
CMD_SEARCH_RESULT_MATCHED = 143
CMD_DOC_TERM = 160
CMD_DOC_VALUE = 161
CMD_DOC_INDEX = 162
CMD_INDEX_REQUEST = 163
CMD_IMPORT_HEADER = 191
CMD_SEARCH_SET_SORT = 192
CMD_SEARCH_SET_CUT = 193
CMD_SEARCH_SET_NUMERIC = 194
CMD_SEARCH_SET_COLLAPSE = 195
CMD_SEARCH_KEEPALIVE = 196
CMD_SEARCH_SET_FACETS = 197
CMD_SEARCH_SCWS_SET = 198
CMD_SEARCH_SET_CUTOFF = 199
CMD_SEARCH_SET_MISC = 200
CMD_QUERY_INIT = 224
CMD_QUERY_PARSE = 225
CMD_QUERY_TERM = 226
CMD_QUERY_RANGEPROC = 227
CMD_QUERY_RANGE = 228
CMD_QUERY_VALCMP = 229
CMD_QUERY_PREFIX = 230
CMD_QUERY_PARSEFLAG = 231
CMD_SORT_TYPE_RELEVANCE = 0
CMD_SORT_TYPE_DOCID = 1
CMD_SORT_TYPE_VALUE = 2
CMD_SORT_TYPE_MULTI = 3
CMD_SORT_TYPE_MASK = 0x3f
CMD_SORT_FLAG_RELEVANCE = 0x40
CMD_SORT_FLAG_ASCENDING = 0x80
CMD_QUERY_OP_AND = 0
CMD_QUERY_OP_OR = 1
CMD_QUERY_OP_AND_NOT = 2
CMD_QUERY_OP_XOR = 3
CMD_QUERY_OP_AND_MAYBE = 4
CMD_QUERY_OP_FILTER = 5
CMD_RANGE_PROC_STRING = 0
CMD_RANGE_PROC_DATE = 1
CMD_RANGE_PROC_NUMBER = 2
CMD_VALCMP_LE = 0
CMD_VALCMP_GE = 1
CMD_PARSE_FLAG_BOOLEAN = 1
CMD_PARSE_FLAG_PHRASE = 2
CMD_PARSE_FLAG_LOVEHATE = 4
CMD_PARSE_FLAG_BOOLEAN_ANY_CASE = 8
CMD_PARSE_FLAG_WILDCARD = 16
CMD_PARSE_FLAG_PURE_NOT = 32
CMD_PARSE_FLAG_PARTIAL = 64
CMD_PARSE_FLAG_SPELLING_CORRECTION = 128
CMD_PARSE_FLAG_SYNONYM = 256
CMD_PARSE_FLAG_AUTO_SYNONYMS = 512
CMD_PARSE_FLAG_AUTO_MULTIWORD_SYNONYMS = 1536
CMD_PREFIX_NORMAL = 0
CMD_PREFIX_BOOLEAN = 1
CMD_INDEX_WEIGHT_MASK = 0x3f
CMD_INDEX_FLAG_WITHPOS = 0x40
CMD_INDEX_FLAG_SAVEVALUE = 0x80
CMD_INDEX_FLAG_CHECKSTEM = 0x80
CMD_VALUE_FLAG_NUMERIC = 0x80
CMD_INDEX_REQUEST_ADD = 0
CMD_INDEX_REQUEST_UPDATE = 1
CMD_INDEX_SYNONYMS_ADD = 0
CMD_INDEX_SYNONYMS_DEL = 1
CMD_SEARCH_MISC_SYN_SCALE = 1
CMD_SEARCH_MISC_MATCHED_TERM = 2
CMD_SCWS_GET_VERSION = 1
CMD_SCWS_GET_RESULT = 2
CMD_SCWS_GET_TOPS = 3
CMD_SCWS_HAS_WORD = 4
CMD_SCWS_GET_MULTI = 5
CMD_SCWS_SET_IGNORE = 50
CMD_SCWS_SET_MULTI = 51
CMD_SCWS_SET_DUALITY = 52
CMD_SCWS_SET_DICT = 53
CMD_SCWS_ADD_DICT = 54
CMD_ERR_UNKNOWN = 600
CMD_ERR_NOPROJECT = 401
CMD_ERR_TOOLONG = 402
CMD_ERR_INVALIDCHAR = 403
CMD_ERR_EMPTY = 404
CMD_ERR_NOACTION = 405
CMD_ERR_RUNNING = 406
CMD_ERR_REBUILDING = 407
CMD_ERR_WRONGPLACE = 450
CMD_ERR_WRONGFORMAT = 451
CMD_ERR_EMPTYQUERY = 452
CMD_ERR_TIMEOUT = 501
CMD_ERR_IOERR = 502
CMD_ERR_NOMEM = 503
CMD_ERR_BUSY = 504
CMD_ERR_UNIMP = 505
CMD_ERR_NODB = 506
CMD_ERR_DBLOCKED = 507
CMD_ERR_CREATE_HOME = 508
CMD_ERR_INVALID_HOME = 509
CMD_ERR_REMOVE_HOME = 510
CMD_ERR_REMOVE_DB = 511
CMD_ERR_STAT = 512
CMD_ERR_OPEN_FILE = 513
CMD_ERR_TASK_CANCELED = 514
CMD_ERR_XAPIAN = 515
CMD_OK_INFO = 200
CMD_OK_PROJECT = 201
CMD_OK_QUERY_STRING = 202
CMD_OK_DB_TOTAL = 203
CMD_OK_QUERY_TERMS = 204
CMD_OK_QUERY_CORRECTED = 205
CMD_OK_SEARCH_TOTAL = 206
CMD_OK_RESULT_BEGIN = CMD_OK_SEARCH_TOTAL
CMD_OK_RESULT_END = 207
CMD_OK_TIMEOUT_SET = 208
CMD_OK_FINISHED = 209
CMD_OK_LOGGED = 210
CMD_OK_RQST_FINISHED = 250
CMD_OK_DB_CHANGED = 251
CMD_OK_DB_INFO = 252
CMD_OK_DB_CLEAN = 253
CMD_OK_PROJECT_ADD = 254
CMD_OK_PROJECT_DEL = 255
CMD_OK_DB_COMMITED = 256
CMD_OK_DB_REBUILD = 257
CMD_OK_LOG_FLUSHED = 258
CMD_OK_DICT_SAVED = 259
CMD_OK_RESULT_SYNONYMS = 280
CMD_OK_SCWS_RESULT = 290
CMD_OK_SCWS_TOPS = 291
PACKAGE_BUGREPORT = "http://www.xunsearch.com/bugs"
PACKAGE_NAME = "xunsearch"
PACKAGE_TARNAME = "xunsearch"
PACKAGE_URL = ""
PACKAGE_VERSION = "1.4.8"
XS_SOCKET_TIMEOUT = 30
XS_LIB_ROOT = os.path.dirname(__file__)


class XSException(Exception):
    
    def getCode(self):
        if len(self.args) > 1:
            return self.args[1]
        else:
            return 0

    def getMessage(self):
        return self.args[0] if len(self.args) >= 1 else ''


class XSErrorException(XSException):
    pass


class XSComponent(object):

    def __getattr__(self, name):
        if name[0:2] == '__':
            return object.__getattr__(self, name)
        name = name.title()
        getter = 'get' + name
        if getter in dir(self):
            return getattr(self, getter)()
        msg = 'Write-only' if hasattr(self, 'set' + name) else 'Undefined'
        msg += ' property: %s::%s'  % (self.__class__.__name__, name)
        raise XSException(msg)

class XS(XSComponent):

    _index = None
    _search = None
    _scws = None
    _scheme = None
    _bindScheme = None
    _config = None
    _lastXS = None

    def __init__(self, fname):
        if len(fname) < 255 and not os.path.isfile(fname):
            _file = os.path.join(XS_LIB_ROOT, 'conf', fname + '.ini')
            if os.path.isfile(_file):
                fname = _file
        self._loadIniFile(fname)
        self._lastXS = self

    def __del__(self):
        self._index = None
        self._search = None

    def getLastXS(self):
        return self._lastXS

    def getScheme(self):
        return self._scheme

    def setScheme(self, fs):
        fs.checkValid(True)
        self._scheme = fs
        if self._search is None:
            self._search.markResetScheme()

    def restoreScheme(self):
        if self._scheme is not self._bindScheme:
            self._scheme =self._bindScheme
            if self._search is not None:
                self._search.markResetScheme()

    def getConfig(self):
        return self._config

    def getName(self):
        return self._config['project.name']

    def setName(self, name):
        self._config['project.name'] = name

    def getDefaultCharset(self):
        try:
            return self._config['project.default_charset'].upper()
        except KeyError:
            return 'UTF-8'

    def setDefaultCharset(self, charset):
        self._config['project.default_charset'] = charset.upper()

    def getIndex(self):
        if self._index is None:
            adds = []
            conn = self._config['server.index'] if 'server.index' in self._config else '8383'
            pos = conn.find(';')
            if pos != -1:
                adds = conn[pos + 1:].split('')
                conn = conn[0:pos]
            self._index = XSIndex(conn, self)
            self._index.setTimeout(0)
            for conn in adds:
                conn = conn.strip()
                if conn is not '':
                    self._index.addServer(conn).setTimeout(0)
        return self._index

    def getSearch(self):
        if self._search is None:
            conns = []
            if 'server.search' not in self._config:
                conns.append('8384')    
            else:
                for conn in self._config['server.search'].split(';'):
                    conn = conn.strip()
                    if conn != '':
                        conns.append(conn)
            i = 0
            for conn in conns:
                try:
                    self._search = XSSearch(conn, self)
                    self._search.setCharset(self.getDefaultCharset())
                    return self._search
                except XSException as e:
                    if (i + 1) == len(conns):
                        raise XSException(*e.args)
        return self._search

    def getScwsServer(self):
        if self._scws is None:
            try:
                conn = self._config['server.search']
            except KeyError:
                conn = 8384
            self._scws = XSServer(conn, self)
        return self._scws

    def getFieldId(self):
        return self._scheme.getFieldId()

    def getFieldTitle(self):
        return self._scheme.getFieldTitle()

    def getFieldBody(self):
        return self._scheme.getFieldBody()

    def getField(self, name, throw=True):
        return self._scheme.getField(name, throw)

    def getAllFields(self):
        return self._scheme.getAllFields()

    @staticmethod
    def autoload(name):
        __import__(name)

    @staticmethod
    def convert(data, to, _from):
        if to == _from:
            return data
        if isinstance(data, dict):
            for key in data:
                data[key] = self.convert(data[key], to, _from)
            return data

        if isinstance(data, basestring) and re.search('[\x81-\xfe]', data):
            try:
                return data.decode(to).encode(_from)
            except Exception:
                raise XSException('Cann\'t find the mbstring or iconv extension to convert encoding')
        return data

    def _parseIniData(self, data):
        ret = OrderedDict()
        cur = ret
        lines = data.split("\n")

        for line in lines:
            if line == '' or line[0] == '' or line[0] == '#':
                continue
            line = line.strip()
            if line == '':
                continue
            if line[0] == '[' and line[-1] == ']':
                sec = line[1:-1]
                ret[sec] = {}
                cur = ret[sec]
                continue
            pos = line.find('=')
            if pos == -1:
                continue
            key = line[0:pos].strip()
            cur[key] = line[pos + 1:].strip(" '\t\"")

        return ret

    def _loadIniFile(self, fname):
        cache = False
        cache_write = ''
        if len(fname) < 255 and os.path.isfile(fname):
            with open(fname, 'r') as fp:
                data = fp.read()
        else:
            data = fname
            fname = hashlib.md5(fname).hexdigest()[8:16] + '.ini'
        
        self._config = self._parseIniData(data)
        if not self._config:
            raise XSException('Failed to parse project config file/string: \'' + fname[0:10] + '...\'')
        scheme = XSFieldScheme()
        for key in self._config:
            value = self._config[key]
            if isinstance(value, dict):
                scheme.addField(key, value)
        scheme.checkValid(True)
        
        if 'project.name' not in self._config:
            self._config['project.name'] = os.path.basename(fname, '.ini')
        self._scheme = self._bindScheme = scheme


class XSDocument(object):

    _data = None
    _terms = None
    _texts = None
    _charset = None
    _meta = None
    _resSize = 20
    _resFormat = 'IIIif'

    def __init__(self, d=None, p=None):
        self._data = {}
        if isinstance(p, dict):
            self._data = p
        elif isinstance(p, basestring):
            if len(p) != self._resSize:
                self.setCharset(p)
                return
            docid, rank, ccount, percent, weight = unpack(self._resFormat, p)
            self._meta = {
                'docid': docid,
                'rank': rank,
                'ccount': ccount,
                'percent': percent,
                'weight': weight,
            }
        if d is not None and isinstance(d, basestring):
            self.setCharset(d)
    
    def __getattr__(self, name):
        if name[0:2] == '__' or name in dir(self):
            return object.__getattr__(self, name)
        return self.get(name)

    def __setattr__(self, name, value):
        if name in dir(self):
            return object.__setattr__(self, name, value)
        if self._meta is not None:
            raise XSException('Magick property of result document is read-only')
        self.setField(name, value)

    def __call__(self, name, args):
        if self._meta is not None:
            name = name.lower()
            if name in self._meta:
                return self._meta[name]
        raise XSException('Call to undefined method `%s.%s()\'' % (self.__class__.__name__, name))
    
    def get(self, name, default=None):
        if name not in self._data:
            return default
        return self._autoConvert(self._data[name])

    def getCharset(self):
        return self._charset
    
    def setCharset(self, charset):
        self._charset = charset.upper()
        if self._charset == 'UTF8':
            self._charset = 'UTF-8'
        return self
    
    def getFields(self):
        return self._data
    
    def setFields(self, data):
        if data is None:
            self._data = {}
            self._meta = self._terms = self._texts = None
        else:
            self._data.update(data)

    def setField(self, name, value, isMeta=False):
        if self._data is None:
            self._data = {}
        if value is None:
            if isMeta:
                del self._meta[name]
            else:
                del self._data[name]
        else:
            if isMeta:
                self._meta[name] = value
            else:
                self._data[name] = value
    
    def f(self, name):
        return str(self.__getattr__(str(name)))
    
    def getAddTerms(self, field):
        field = str(field)
        if self._terms is None or field not in self._terms:
            return None
        terms = {}
        for key in self._terms[field]:
            weight = self._terms[field][key]
            key = self._autoConvert(key)
            terms[key] = weight
        return terms
    
    def getAddIndex(self, field):
        field = str(field)
        if self._texts is None or field not in self._texts:
            return None
        return self._autoConvert(self._texts[field])
    
    def addTerm(self, field, term, weight=1):
        field = str(field)
        if not isinstance(self._terms, dict):
            self._terms = {}
        
        if field not in self._terms:
            self._terms[field] = {term: weight}
        elif term not in self._terms[field]:
            self._terms[field][term] = weight
        else:
            self._terms[field][term] += weight

    def addIndex(self, field, text):
        field = str(field)
        if not isinstance(self._texts, dict):
            self._texts = {}
        
        if field not in self._texts:
            self._texts[field] = str(text)
        else:
            self._texts[field] += "\n" + str(text)

    def getIterator(self):
        if self._charset is not None and self._charset != 'UTF-8':
            _from = self._charset if self._meta is None else 'UTF-8'
            to = 'UTF-8' if self._meta is None else self._charset
            return XS.convert(self._data, to, _from)
        return self._data

    def offsetExists(self, name):
        return name in self._data

    def offsetGet(self, name):
        return self.__get__(name)

    def offsetSet(self, name, value):
        if name is not None:
            self.__set__(str(name), value)

    def offsetUnset(self, name):
        del self._data[name]
    
    def beforeSubmit(self, index):
        if self._charset is None:
            self._charset = index.xs.getDefaultCharset()
        return True
    
    def afterSubmit(self, index):
        pass

    def _autoConvert(self, value):
        if self._charset is None or self._charset == 'UTF-8' \
           or isinstance(value, basestring) or not re.search('[\x81-\xfe]', value):
            return value
        _from = self._charset if self._meta is None else 'UTF-8'
        _to = 'UTF-8' if self._meta is None else self._charset
        return XS.convert(value, _to, _from)


class XSFieldScheme:

    MIXED_VNO = 255
    _logger = None

    def __init__(self):
        self._fields = OrderedDict()
        self._typeMap = {}
        self._vnoMap = {}

    def __str__(self):
        _str = ''
        for field in self._fields:
            _str += field.toConfig() + "\n"
        return _str
    
    def getFieldId(self):
        if XSFieldMeta.TYPE_ID in self._typeMap:
            name = self._typeMap[XSFieldMeta.TYPE_ID]
            return self._fields[name]
        return False
    
    def getFieldTitle(self):
        if XSFieldMeta.TYPE_TITLE in self._typeMap:
            name = self._typeMap[XSFieldMeta.TYPE_TITLE]
            return self._fields[name]
        for name in self._fields:
            field = self._fields[name]
            if field.ctype == XSFieldMeta.TYPE_STRING and not field.isBoolIndex():
                return field
        return False
    
    def getFieldBody(self):
        if XSFieldMeta.TYPE_BODY in self._typeMap:
            name = self._typeMap[XSFieldMeta.TYPE_BODY]
            return self._fields[name]
        return False
    
    def getField(self, name, throw=True):
        if isinstance(name, int):
            if name not in self._vnoMap:
                if trow is True:
                    raise XSException('Not exists field with vno: `%s\'' % (name,))
                return False
            name = self._vnoMap[name]
        if name not in self._fields:
            if throw is True:
                raise XSException('Not exists field with name: `%s\'' % (name,))
            return False
        return self._fields[name]

    def getAllFields(self):
        return self._fields
    
    def getVnoMap(self):
        return self._vnoMap
    
    def addField(self, field, config=None):
        if not isinstance(field, XSFieldMeta):
            field = XSFieldMeta(field, config)
        if field.name in self._fields:
            raise XSException('Duplicated field name: `%s`' % (field.name,))
        if field.isSpeical():
            if field.ctype in self._typeMap:
                prev = self._typeMap[field.ctype]
                raise XSException("Duplicated %s field: `%s` and `%s`" % (config['type'].upper(),
                                  field.name, prev))
            self._typeMap[field.ctype] = field.name
        field.vno = self.MIXED_VNO if (field.ctype == XSFieldMeta.TYPE_BODY) else len(self._vnoMap)
        self._vnoMap[field.vno] = field.name
        if field.ctype == XSFieldMeta.TYPE_ID:
            self._fields.update({field.name: field})
        else:
            self._fields[field.name] = field
        
    def checkValid(self, throw=False):
        if XSFieldMeta.TYPE_ID not in self._typeMap:
            if throw:
                raise XSException('Missing field of type ID')
            return False
        return True
    
    def getIterator(self):
        return self._fields
    
    @staticmethod
    def logger():
        if XSFieldScheme._logger is None:
            scheme = XSFieldScheme()
            scheme.addField('id', {'type': 'id'})
            scheme.addField('pinyin')
            scheme.addField('partial')
            scheme.addField('total', {'type': 'numeric', 'index': 'self'})
            scheme.addField('lastnum', {'type': 'numeric', 'index': 'self'})
            scheme.addField('currnum', {'type': 'numeric', 'index': 'self'})
            scheme.addField('currtag', {'type': 'string'})
            scheme.addField('body', {'type': 'body'})
            XSFieldScheme._logger = scheme
        return XSFieldScheme._logger


class XSFieldMeta:

    MAX_WDF = 0x3f
    TYPE_STRING = 0
    TYPE_NUMERIC = 1
    TYPE_DATE = 2
    TYPE_ID = 10
    TYPE_TITLE = 11
    TYPE_BODY = 12
    FLAG_INDEX_SELF = 0x01
    FLAG_INDEX_MIXED = 0x02
    FLAG_INDEX_BOTH = 0x03
    FLAG_WITH_POSITION = 0x10
    FLAG_NON_BOOL = 0x80 # 强制让该字段参与权重计算 (非布尔)
    name = None
    cutlen = 0
    weight = 1
    ctype = 0
    vno = 0

    def __init__(self, name, config=None):
        self.name = str(name)
        self._tokenizer = XSTokenizer.DFL
        self._flag = 0
        self._tokenizers = {}
        if isinstance(config, dict):
            self._fromConfig(config)
    
    def __str__(self):
        return self.name
    
    def val(self, value):
        if self.ctype == self.TYPE_DATE:
            if value.isdigit() or len(value) != 8:
                value = time.strftime('%Y%m%d', value)
                #value = date('Ymd', is_numeric(value) ? value : strtotime(value))
        return value
    
    def withPos(self):
        return True if (self._flag & self.FLAG_WITH_POSITION) else False

    def isBoolIndex(self):
        if (self._flag & self.FLAG_NON_BOOL):
            return False
        return (not self.hasIndex() or self._tokenizer is not XSTokenizer.DFL)
    
    def isNumeric(self):
        return self.ctype == self.TYPE_NUMERIC
    
    def isSpeical(self):
        return (self.ctype == self.TYPE_ID or self.ctype == self.TYPE_TITLE or self.ctype == self.TYPE_BODY)
    
    def hasIndex(self):
        return True if (self._flag & self.FLAG_INDEX_BOTH) else False
    
    def hasIndexMixed(self):
        return True if (self._flag & self.FLAG_INDEX_MIXED) else False
    
    def hasIndexSelf(self):
        return True if (self._flag & self.FLAG_INDEX_SELF) else False
    
    def hasCustomTokenizer(self):
        return self._tokenizer is not XSTokenizer.DFL
    
    def getCustomTokenizer(self):
        if self._tokenizer in self._tokenizers:
            return self._tokenizers[self._tokenizer]
        else:
            pos1 = self._tokenizer.find('(')
            pos2 = self._tokenizer.find(')', pos1 + 1) if pos1 != -1 else -1
            if pos1 != -1 and pos2 != -1:
                name = 'XSTokenizer' + self._tokenizer[0:pos1].strip().title()
                arg = self._tokenizer[pos1+1:pos2]
            else:
                name = 'XSTokenizer' + self._tokenizer.title()
                arg = None
            module_name = 'xunsearch.' + name
            if name not in globals() and module_name not in sys.modules:
                try:
                    __import__(module_name)
                except ImportError:
                    raise XSException('Undefined custom tokenizer `' + self._tokenizer + '\' for field `' + self.name + '\'')
            func = getattr(sys.modules[module_name], name) if name not in globals() else globals()[name]
            obj = func() if arg is None else func(arg)
            if not isinstance(obj, XSTokenizer):
                raise XSException(name + ' for field `' + self.name + '\' dose not implement the interface: XSTokenizer')
            self._tokenizers[self._tokenizer] = obj
            return obj

    def toConfig(self):
        _str = "[" + self.name + "]\n"
        if self.ctype is self.TYPE_NUMERIC:
            _str += "type = numeric\n"
        elif self.ctype is self.TYPE_DATE:
            _str += "type = date\n"
        elif self.ctype is self.TYPE_ID:
            _str += "type = id\n"
        elif self.ctype is self.TYPE_TITLE:
            _str += "type = title\n"
        elif self.ctype is self.TYPE_BODY:
            _str += "type = body\n"
        index = (self._flag & self.FLAG_INDEX_BOTH)
        if self.ctype is not self.TYPE_BODY and index:
            if index is self.FLAG_INDEX_BOTH:
                if self.ctype is not self.TYPE_TITLE:
                    _str += "index = both\n"
            elif index is self.FLAG_INDEX_MIXED:
                _str += "index = mixed\n"
            else:
                if self.ctype is not self.TYPE_ID:
                    _str += "index = self\n"
            
        if self.ctype is not self.TYPE_ID and self._tokenizer is not XSTokenizer.DFL:
            _str += "tokenizer = " + self._tokenizer + "\n"
        if self.cutlen > 0 and not (self.cutlen == 300 and self.ctype is self.TYPE_BODY):
            _str += "cutlen = " + self.cutlen + "\n"
        if self.weight != 1 and not (self.weight == 5 and self.ctype is self.TYPE_TITLE):
            _str += "weight = " + self.weight + "\n"
        
        if self._flag & self.FLAG_WITH_POSITION:
            if self.ctype is not self.TYPE_BODY and self.ctype is not self.TYPE_TITLE:
                _str += "phrase = yes\n"
        else:
            if self.ctype is self.TYPE_BODY or self.ctype is self.TYPE_TITLE:
                _str += "phrase = no\n"
        
        if self._flag & self.FLAG_NON_BOOL:
            _str += "non_bool = yes\n"
        return _str
    
    def _fromConfig(self, config):
        if 'type' in config:
            predef = 'TYPE_' + config['type'].upper()
            if hasattr(self, predef):
                self.ctype = getattr(self, predef)
                if self.ctype == self.TYPE_ID:
                    self._flag = self.FLAG_INDEX_SELF
                    self._tokenizer = 'full'
                elif self.ctype == self.TYPE_TITLE:
                    self._flag = self.FLAG_INDEX_BOTH | self.FLAG_WITH_POSITION
                    self.weight = 5
                elif self.ctype == self.TYPE_BODY:
                    self.vno = XSFieldScheme.MIXED_VNO
                    self._flag = self.FLAG_INDEX_SELF | self.FLAG_WITH_POSITION
                    self.cutlen = 300
        if 'index'in config and self.ctype != self.TYPE_BODY:
            predef = 'FLAG_INDEX_' + config['index'].upper()
            if hasattr(self, predef):
                self._flag &= ~ self.FLAG_INDEX_BOTH
                self._flag |= getattr(self, predef)
        
            if self.ctype == self.TYPE_ID:
                self._flag |= self.FLAG_INDEX_SELF
        if 'cutlen' in config:
            self.cutlen = int(config['cutlen'])
        
        if 'weight' in config and self.ctype != self.TYPE_BODY:
            self.weight = int(config['weight']) & self.MAX_WDF
        
        if 'phrase' in config:
            if config['phrase'].lower() == 'yes':
                self._flag |= self.FLAG_WITH_POSITION
            elif config['phrase'].lower()  == 'no':
                self._flag &= ~ self.FLAG_WITH_POSITION

        if 'non_bool' in config:
            if config['non_bool'].lower() == 'yes':
                self._flag |= self.FLAG_NON_BOOL
            elif config['non_bool'].lower() == 'no':
                self._flag &= ~ self.FLAG_NON_BOOL

        if 'tokenizer' in config and self.ctype != self.TYPE_ID \
            and config['tokenizer'] != 'default':
            self._tokenizer = config['tokenizer']


class XSServer(XSComponent):

    FILE = 0x01
    BROKEN = 0x02
    xs = None

    def __init__(self, conn=None, xs=None):
        self._sock = self._sockfp = self._conn = self._project = self._sendBuffer = None
        self.xs = xs
        if conn is not None:
            self.open(conn)

    def __del__(self):
        self.xs = None
        self.close()

    def open(self, conn):
        self.close()
        self._conn = conn
        self._flag = self.BROKEN
        self._sendBuffer = ''
        self._project = None
        self.connect()
        self._flag ^= self.BROKEN
        if isinstance(self.xs, XS):
            self.setProject(self.xs.getName())
    
    def reopen(self, force=False):
        if (self._flag & self.BROKEN) or force is True:
            self.open(self._conn)
        return self

    def close(self, ioerr=False):
        if self._sock and not (self._flag & self.BROKEN):
            if not ioerr and self._sendBuffer != '':
                self.write(self._sendBuffer)
                self._sendBuffer = ''
            if not ioerr and not (self._flag & self.FILE):
                cmd = XSCommand(CMD_QUIT)
                self._sockfp.write(cmd)
            self._sockfp.close()
            self._flag |= self.BROKEN
    
    def getConnString(self):
        _str = self._conn
        if isinstance(_str, int) or _str.isdigit():
            _str = 'localhost:%s' % (_str,) 
        elif ':' in _str:
            _str = 'unix://%s' % (_str,)
        return _str

    def getSocket(self):
        return self._sockfp

    def getProject(self):
        return self._project

    def setProject(self, name, home=''):
        if name is not self._project:
            cmd = {'cmd': CMD_USE, 'buf': name, 'buf1': home}
            self.execCommand(cmd, CMD_OK_PROJECT)
            self._project = name
    
    def setTimeout(self, sec):
        cmd = {'cmd': CMD_TIMEOUT, 'arg': sec}
        self.execCommand(cmd, CMD_OK_TIMEOUT_SET)

    def execCommand(self, cmd, res_arg=CMD_NONE, res_cmd=CMD_OK):
        if not isinstance(cmd, XSCommand):
            cmd = XSCommand(cmd)
        if cmd.cmd & 0x80:
            self._sendBuffer += str(cmd)
            return True
        buf = self._sendBuffer + str(cmd)
        self._sendBuffer = ''
        self.write(buf)
        if self._flag & self.FILE:
            return True
        res = self.getRespond()
        if res.cmd == CMD_ERR and res_cmd != CMD_ERR:
            raise XSException(res.buf, res.arg)
        if res.cmd != res_cmd or (res_arg != CMD_NONE and res.arg != res_arg):
            raise XSException('Unexpected respond {CMD:%s, ARG:%s}' % (res.cmd, res.arg))
        return res

    def sendCommand(self, cmd):
        if not isinstance(cmd, XSCommand):
            cmd = XSCommand(cmd)
        self.write(str(cmd))

    def getRespond(self):
        buf = self.read(8)
        hdr = unpack('BBBBI', buf)
        res = XSCommand(*hdr)
        res.buf = self.read(hdr[4])
        res.buf1 = self.read(hdr[3])
        return res

    def hasRespond(self):
        if self._sock is None or self._flag & (self.BROKEN | self.FILE):
            return False
        wfds = xfds = []
        rfds = [self._sockfp]
        res = select.select(rfds, wfds, xfds, 0, 0)
        return res > 0

    def write(self, buf, _len=0):
        buf = str(buf)
        size = len(buf)
        if _len == 0 and size == 0:
            return True
        _len = size
        self.check()
        while 1:
            try:
                self._sockfp.write(buf[0:_len])
                _bytes = _len
            except socket.timeout:
                _bytes = None
            except:
                _bytes = False
            if not _bytes or _bytes == 0 or _bytes == _len:
                break
            _len -= _bytes
            buf = buf[_bytes:]
        if not _bytes or _bytes == 0:
            reason = 'timeout' if _bytes is None else ('closed' if self._sockfp.closed else 'unknown')
            self.close(True)
            msg = 'Failed to recv the data from server completely '
            msg += '(SIZE:%s/%s, REASON:%s)' % (size - _len, size, reason)
            raise XSException(msg)

    def read(self, _len):
        if _len == 0:
            return ''
        self.check()
        buf = ''
        size = _len
        meta = {}
        while 1:
            try:
                _bytes = self._sockfp.read(_len)
            except socket.timeout:
                _bytes = None
            except:
                _bytes = False
            if not _bytes:
                break
            _len -= len(_bytes)
            buf += _bytes
            if _len == 0:
                return buf
        reason = 'timeout' if _bytes is None else ('closed' if self._sockfp.closed else 'unknown')
        self.close(True)
        msg = 'Failed to recv the data from server completely '
        msg += '(SIZE:%s/%s, REASON:%s)' % (size - _len, size, reason)
        raise XSException(msg)

    def check(self):
        if self._sockfp is None:
            raise XSException('No server connection')
        if self._flag & self.BROKEN:
            raise XSException('Broken server connection')
    
    def connect(self):
        conn = self._conn
        if isinstance(conn, int) or conn.isdigit():
            host = '127.0.0.1'
            port = int(conn)
            af = socket.AF_INET
            addr = (host, port)
        elif conn[0:7] in ('file://', 'unix://'):
            conn = conn[7:]
            af = socket.AF_UNIX
            addr = conn
            self._flag |= self.FILE
        elif ':' in conn:
            pos = conn.find(':')
            host = conn[0:pos]
            port = int(conn[pos + 1:])
            af = socket.AF_INET
            addr = (host, port)
        else:
            af = socket.AF_UNIX
            addr = conn
        try:
            sock = socket.socket(af, socket.SOCK_STREAM)
            sock.settimeout(XS_SOCKET_TIMEOUT)
            sock.connect(addr)
            self._sock = sock
            self._sockfp = sock.makefile(bufsize=0)
        except socket.error, msg:
            if sock:
                sock.close()
            raise XSException('connection faile: %s(%s:%s)' % (msg, host, port))


class XSIndex(XSServer):

    _buf = ''
    _bufSize = 0
    _rebuild = False
    _adds = {}

    def addServer(self, conn):
        srv = XSServer(conn, self.xs)
        self._adds.append(srv)
        return srv
    
    def execCommand(self, cmd, res_arg=CMD_NONE, res_cmd=CMD_OK):
        res = super(XSIndex, self).execCommand(cmd, res_arg, res_cmd)
        for srv in self._adds:
            srv.execCommand(cmd, res_arg, res_cmd)
        return res
    
    def clean(self):
        self.execCommand(CMD_INDEX_CLEAN_DB, CMD_OK_DB_CLEAN)
        return self

    def add(self, doc):
        return self.update(doc, True)

    @staticmethod
    def getTermWeightMultiple(value, term):
        if value == term:
            return 50
        if isinstance(term, (list, tuple)):
            if value == term[0]:
                return 50
            import difflib
            parent = difflib.SequenceMatcher(None, value, term[0]).quick_ratio() * 100
            return (10 + 10 * parent/100) * len(term[0]) / len(value)
        return 1
    
    def update(self, doc, add=False):
        if doc.beforeSubmit(self) is False:
            return self
        fid = self.xs.getFieldId()
        key = doc.f(fid)
        if key is None or key == '':
            raise XSException('Missing value of primary key (FIELD: %s)' % (fid,))
        cmd = XSCommand(CMD_INDEX_REQUEST, CMD_INDEX_REQUEST_ADD)
        if add is not True:
            cmd.arg1 = CMD_INDEX_REQUEST_UPDATE
            cmd.arg2 = fid.vno
            cmd.buf = key
        cmds = [cmd]
        _fields = self.xs.getAllFields()
        for k in _fields:
            field = _fields[k]
            value = doc.f(field)
            if value is not None:
                varg = CMD_VALUE_FLAG_NUMERIC if field.isNumeric() else 0
                value = field.val(value)
                if not field.hasCustomTokenizer():
                    wdf = field.weight | (CMD_INDEX_FLAG_WITHPOS if field.withPos() else 0)
                    if field.hasIndexMixed():
                        cmds.append(
                            XSCommand(CMD_DOC_INDEX, wdf, XSFieldScheme.MIXED_VNO, value)
                        )
                    
                    if field.hasIndexSelf():
                        wdf |= 0 if field.isNumeric() else CMD_INDEX_FLAG_SAVEVALUE
                        cmds.append(
                            XSCommand(CMD_DOC_INDEX, wdf, field.vno, value)
                        )
                    if field.hasIndexSelf() or field.isNumeric():
                        cmds.append(
                            XSCommand(CMD_DOC_VALUE, varg, field.vno, value)
                        )
                else:
                    if field.hasIndex():
                        obj = field.getCustomTokenizer()
                        isChipstep = False
                        hasSplit = True if '€€' in value else False
                        if 'xunsearch.XSTokenizerChipstep' in sys.modules:
                            isChipstep = True
                        terms = obj.getTokens(value, doc)
                        if field.hasIndexSelf():
                            wdf = 1 if field.isBoolIndex() else (field.weight | CMD_INDEX_FLAG_CHECKSTEM)
                            if hasSplit and isChipstep:
                                count = len(value.split('€€'))
                                tmp_terms = []
                                for i in xrange(0, count):
                                    tmp_terms.append(terms[i])
                                terms = tmp_terms

                            for term in terms:
                                if len(term) > 200:
                                    continue
                                if isinstance(term, (list, tuple)):
                                    term = term[0]
                                term = term.lower()
                                cmds.append(
                                    XSCommand(CMD_DOC_TERM, wdf, field.vno, term)
                                )
                        if field.hasIndexMixed():
                            if isChipstep:
                                wdf = 1 if field.isBoolIndex() else (field.weight | CMD_INDEX_FLAG_CHECKSTEM)
                                value_arr = value.split('€€')
                                value_arr_count = len(value_arr)
                                k = 0
                                for val in value_arr:   
                                    split_terms = terms[k] if value_arr_count > 1 else terms
                                    count = len(split_terms)
                                    for term in split_terms:
                                        weight = self.getTermWeightMultiple(val, term)* wdf / count
                                        if isinstance(term, (list, tuple)):
                                            term = term[0]
                                        if len(term) > 200:
                                            continue
                                        term = term.lower()
                                        cmds.append(
                                            XSCommand(CMD_DOC_INDEX, weight, XSFieldScheme.MIXED_VNO, term)
                                        )
                                    k += 1
                            else:
                                mtext = ' '.join(terms)
                                cmds.append(
                                    XSCommand(CMD_DOC_INDEX, field.weight, XSFieldScheme.MIXED_VNO, mtext)
                                )
                    cmds.append(XSCommand(CMD_DOC_VALUE, varg, field.vno, value))
            terms = doc.getAddTerms(field)
            if terms is not None:
                wdf1 = 0 if field.isBoolIndex() else CMD_INDEX_FLAG_CHECKSTEM
                for term in terms:
                    wdf = terms[term]
                    term = term.lower()
                    if len(term) > 200:
                        continue
                    wdf2 = 1 if field.isBoolIndex() else wdf * field.weight
                    while wdf2 > XSFieldMeta.MAX_WDF:
                        cmds.append(XSCommand(CMD_DOC_TERM, wdf1 | XSFieldMeta.MAX_WDF, field.vno, term))
                        wdf2 -= XSFieldMeta.MAX_WDF
                    cmds.append(XSCommand(CMD_DOC_TERM, wdf1 | wdf2, field.vno, term))
            text = doc.getAddIndex(field)
            if text is not None:
                if not field.hasCustomTokenizer():
                    wdf = field.weight | (CMD_INDEX_FLAG_WITHPOS if field.withPos() else 0)
                    cmds.append(XSCommand(CMD_DOC_INDEX, wdf, field.vno, text))
                else:
                    wdf = 1 if field.isBoolIndex() else (field.weight | CMD_INDEX_FLAG_CHECKSTEM)
                    terms = field.getCustomTokenizer().getTokens(text, doc)
                    for term in terms:
                        if len(term) > 200:
                            continue
                        term = term.lower()
                        cmds.append(XSCommand(CMD_DOC_TERM, wdf, field.vno, term))
        cmds.append(XSCommand(CMD_INDEX_SUBMIT))
        if self._bufSize > 0:
            self._appendBuffer(''.join([str(cmd) for cmd in cmds]))
        else:
            for i in xrange(0, len(cmds)):
                self.execCommand(cmds[i])
            self.execCommand(cmds[i], CMD_OK_RQST_FINISHED)
        doc.afterSubmit(self)
        return self
    
    def delete(self, term, field=None):
        field = self.xs.getFieldId() if field is None else self.xs.getField(field)
        cmds = []
        terms = list(set(term)) if isinstance(term, (list, tuple)) else list(term)
        terms = XS.convert(terms, 'UTF-8', self.xs.getDefaultCharset())
        for term in terms:
            cmds.append(XSCommand(CMD_INDEX_REMOVE, 0, field.vno, term.lower()))
        
        if self._bufSize > 0:
            self._appendBuffer(''.join([str(cmd) for cmd in cmds]))
        elif len(cmds) == 1:
            self.execCommand(cmds[0], CMD_OK_RQST_FINISHED)
        else:
            cmd = {'cmd': CMD_INDEX_EXDATA, 'buf': ''.join(cmds)}
            self.execCommand(cmd, CMD_OK_RQST_FINISHED)
        return self
    
    def addExdata(self, data, check_file=True):
        if len(data) < 255 and check_file and os.path.isfile(data):
            with open(data) as fp:
                data = fp.read()
            if not data:
                raise XSException('Failed to read exdata _from file')
        first = ord(data[0:1])
        if first != CMD_IMPORT_HEADER \
            and first != CMD_INDEX_REQUEST and first != CMD_INDEX_SYNONYMS \
            and first != CMD_INDEX_REMOVE and first != CMD_INDEX_EXDATA:
            raise XSException('Invalid start command of exdata (CMD: %s )' % (first,))
        cmd = {'cmd': CMD_INDEX_EXDATA, 'buf': data}
        self.execCommand(cmd, CMD_OK_RQST_FINISHED)
        return self
    
    def addSynonym(self, raw, synonym):
        raw = str(raw)
        synonym = str(synonym)
        if raw != '' and synonym != '':
            cmd = XSCommand(CMD_INDEX_SYNONYMS, CMD_INDEX_SYNONYMS_ADD, 0, raw, synonym)
            if self._bufSize > 0:
                self._appendBuffer(str(cmd))
            else:
                self.execCommand(cmd, CMD_OK_RQST_FINISHED)
        return self
    
    def delSynonym(self, raw, synonym=None):
        raw = str(raw)
        synonym = '' if synonym is None else str(synonym)
        if raw != '':
            cmd = XSCommand(CMD_INDEX_SYNONYMS, CMD_INDEX_SYNONYMS_DEL, 0, raw, synonym)
            if self._bufSize > 0:
                self._appendBuffer(str(cmd))
            else:
                self.execCommand(cmd, CMD_OK_RQST_FINISHED)
        return selfs
    
    def setScwsMulti(self, level):
        level = int(level)
        if level >= 0 and level < 16:
            cmd = {'cmd': CMD_SEARCH_SCWS_SET, 'arg1': CMD_SCWS_SET_MULTI, 'arg2': level}
            self.execCommand(cmd)
        return self

    def getScwsMulti(self):
        cmd = {'cmd': CMD_SEARCH_SCWS_GET, 'arg1': CMD_SCWS_GET_MULTI}
        res = self.execCommand(cmd, CMD_OK_INFO)
        return int(res.buf)
    
    def openBuffer(self, size=4):
        if self._buf != '':
            self.addExdata(self._buf, False)
        self._bufSize = int(size) << 20
        self._buf = ''
        return self
    
    def closeBuffer(self):
        return self.openBuffer(0)
    
    def beginRebuild(self):
        self.execCommand({'cmd': CMD_INDEX_REBUILD, 'arg1': 0}, CMD_OK_DB_REBUILD)
        self._rebuild = True
        return self
    
    def endRebuild(self):
        if self._rebuild is True:
            self._rebuild = False
            self.execCommand({'cmd': CMD_INDEX_REBUILD, 'arg1': 1}, CMD_OK_DB_REBUILD)
        return self
    
    def stopRebuild(self):
        try:
            self.execCommand({'cmd': CMD_INDEX_REBUILD, 'arg1': 2}, CMD_OK_DB_REBUILD)
            self._rebuild = False
        except XSException as e:
            if e.getCode() is not CMD_ERR_WRONGPLACE:
                raise XSException(*e.args)
        return self
    
    def setDb(self, name):
        self.execCommand({'cmd': CMD_INDEX_SET_DB, 'buf': name}, CMD_OK_DB_CHANGED)
        return self
    
    def flushLogging(self):
        try:
            self.execCommand(CMD_FLUSH_LOGGING, CMD_OK_LOG_FLUSHED)
        except XSException as e:
            if e.getCode() is CMD_ERR_BUSY:
                return False
            raise XSException(*e.args)
        return True
    
    def flushIndex(self):
        try:
            self.execCommand(CMD_INDEX_COMMIT, CMD_OK_DB_COMMITED)
        except XSException as e:
            if e.getCode() == CMD_ERR_BUSY or e.getCode() == CMD_ERR_RUNNING:
                return False
            raise XSException(*e.args)
        return True
    
    def getCustomDict(self):
        res = self.execCommand(CMD_INDEX_USER_DICT, CMD_OK_INFO)
        return res.buf
    
    def setCustomDict(self, content):
        cmd = {'cmd': CMD_INDEX_USER_DICT, 'arg1': 1, 'buf': content}
        self.execCommand(cmd, CMD_OK_DICT_SAVED)
    
    def close(self, ioerr=False):
        self.closeBuffer()
        super(XSIndex, self).close(ioerr)
    
    def _appendBuffer(self, buf):
        self._buf += buf
        if len(self._buf) >= self._bufSize:
            self.addExdata(self._buf, False)
            self._buf = ''
    
    def __del__(self):
        if self._rebuild is True:
            try:
                self.endRebuild()
            except Exception as e:
                pass
        for srv in self._adds:
            srv.close()
        self._adds = []
        super(XSIndex, self).__del__()
    

class XSSearch(XSServer):

    PAGE_SIZE = 10
    LOG_DB = 'log_db'
    _defaultOp = CMD_QUERY_OP_AND
    _prefix = None
    _fieldSet = None
    _resetScheme = False
    _query = None
    _terms = []
    _count = None
    _lastCount = None
    _highlight = None
    _curDb = None 
    _curDbs = []
    _lastDb = None
    _lastDbs = []
    _facets = []
    _limit = 0 
    _offset = 0
    _charset = 'UTF-8'

    def open(self, conn):
        super(XSSearch, self).open(conn)
        self._prefix = {}
        self._fieldSet = False
        self._lastCount = False
    
    def setCharset(self, charset):
        self._charset = charset.upper()
        if self._charset == 'UTF8':
            self._charset = 'UTF-8'
        return self
    
    def setFuzzy(self, value=True):
        self._defaultOp = CMD_QUERY_OP_OR if value is True else CMD_QUERY_OP_AND
        return self
    
    def setCutOff(self, percent, weight=0):
        percent = max(0, min(100, int(percent)))
        weight = max(0, (int(weight * 10) & 255))
        cmd = XSCommand(CMD_SEARCH_SET_CUTOFF, percent, weight)
        self.execCommand(cmd)
        return self
    
    def setRequireMatchedTerm(self, value=True):
        arg1 = CMD_SEARCH_MISC_MATCHED_TERM
        arg2 = 1 if value is True else 0
        cmd = XSCommand(CMD_SEARCH_SET_MISC, arg1, arg2)
        self.execCommand(cmd)
        return self
    
    def setAutoSynonyms(self, value=True):
        flag = CMD_PARSE_FLAG_BOOLEAN | CMD_PARSE_FLAG_PHRASE | CMD_PARSE_FLAG_LOVEHATE
        if value is True:
            flag |= CMD_PARSE_FLAG_AUTO_MULTIWORD_SYNONYMS
        cmd = {'cmd': CMD_QUERY_PARSEFLAG, 'arg': flag}
        self.execCommand(cmd)
        return self
    
    def setSynonymScale(self, value):
        arg1 = CMD_SEARCH_MISC_SYN_SCALE
        arg2 = max(0, (int(value * 100) & 255))
        cmd = XSCommand(CMD_SEARCH_SET_MISC, arg1, arg2)
        self.execCommand(cmd)
        return self
    
    def getAllSynonyms(self, limit=0, offset=0, stemmed=False):
        page = pack('II', int(offset), int(limit)) if limit > 0 else ''
        cmd = {'cmd': CMD_SEARCH_GET_SYNONYMS, 'buf1': page}
        cmd['arg1'] = 1 if stemmed == True else 0
        res = self.execCommand(cmd, CMD_OK_RESULT_SYNONYMS)
        ret = {}
        if res.buf:
            for line in res.buf.split("\n"):
                value = "\t".join(line)
                key = value.pop(0)
                ret[key] = value
        return ret
    
    def getQuery(self, query =None):
        query = '' if query is None else self.preQueryString(query)
        cmd = XSCommand(CMD_QUERY_GET_STRING, 0, self._defaultOp, query)
        res = self.execCommand(cmd, CMD_OK_QUERY_STRING)
        if 'VALUE_RANGE' in res.buf:
            regex = '/(VALUE_RANGE) (\d+) (\S+) (\S+?)(?=\))/'
            res.buf = re.sub(regex, self.formatValueRange, res.buf)
        if 'VALUE_GE' in res.buf or 'VALUE_LE' in res.buf:
            regex = '/(VALUE_[GL]E) (\d+) (\S+?)(?=\))/'
            res.buf = re.sub(regex, self.formatValueRange, res.buf)
        return XS.convert(res.buf, self._charset, 'UTF-8')
    
    def setQuery(self, query):
        self.clearQuery()
        if query is not None:
            self._query = query
            self.addQueryString(query)
        return self
    
    def setMultiSort(self, fields, reverse=False, relevance_first=False):
        if not isinstance(fields, dict):
            return self.setSort(fields, not reverse, relevance_first)
        buf = ''
        for key in fields:
            value = fields[key]
            if isinstance(value, bool):
                vno = self.xs.getField(key, True).vno
                asc = value
            else:
                vno = self.xs.getField(value, True).vno
                asc = False
            if vno != XSFieldScheme.MIXED_VNO:
                buf +=  '%s%s' % (chr(vno), chr(1 if asc else 0))

        if not buf:
            _type = CMD_SORT_TYPE_MULTI
            if relevance_first:
                _type |= CMD_SORT_FLAG_RELEVANCE
            if not reverse:
                _type |= CMD_SORT_FLAG_ASCENDING
            cmd = XSCommand(CMD_SEARCH_SET_SORT, _type, 0, buf)
            self.execCommand(cmd)
        return self
    
    def setSort(self, field, asc=False, relevance_first=False):
        if not isinstance(fields, dict):
            return self.setSort(fields, asc, relevance_first)
        if field is None:
            cmd = XSCommand(CMD_SEARCH_SET_SORT, CMD_SORT_TYPE_RELEVANCE)
        else:
            _type = CMD_SORT_TYPE_VALUE
            if relevance_first:
                _type |= CMD_SORT_FLAG_RELEVANCE
            if asc:
                _type |= CMD_SORT_FLAG_ASCENDING
            vno = self.xs.getField(field, True).vno
            cmd = XSCommand(CMD_SEARCH_SET_SORT, _type, vno)
        self.execCommand(cmd)
        return self

    def setDocOrder(self, asc=False):
        _type = CMD_SORT_TYPE_DOCID | (CMD_SORT_FLAG_ASCENDING if asc else 0)
        cmd = XSCommand(CMD_SEARCH_SET_SORT, _type)
        self.execCommand(cmd)
        return self
    
    def setCollapse(self, field, num=1):
        vno = XSFieldScheme.MIXED_VNO if field is None else self.xs.getField(field, True).vno
        _max = min(255, int(num))
        cmd = XSCommand(CMD_SEARCH_SET_COLLAPSE, _max, vno)
        self.execCommand(cmd)
        return self
    
    def addRange(self, field, _from, to):
        if not _from:
            _from = None
        if not to:
            to = None
        if _from is not None or to is not None:
            if len(_from) > 255 or len(to) > 255:
                raise XSException('Value of range is too long')
            vno = self.xs.getField(field).vno
            _from = XS.convert(_from, 'UTF-8', self._charset)
            to = XS.convert(to, 'UTF-8', self._charset)
            if _from is None:
                cmd = XSCommand(CMD_QUERY_VALCMP, CMD_QUERY_OP_FILTER, vno, to, chr(CMD_VALCMP_LE))
            elif to is None:
                cmd = XSCommand(CMD_QUERY_VALCMP, CMD_QUERY_OP_FILTER, vno, _from, chr(CMD_VALCMP_GE))
            else:
                cmd = XSCommand(CMD_QUERY_RANGE, CMD_QUERY_OP_FILTER, vno, _from, to)
            self.execCommand(cmd)
        return self
    
    def addWeight(self, field, term, weight=1):
        return self.addQueryTerm(field, term, CMD_QUERY_OP_AND_MAYBE, weight)
    
    def setFacets(self, field, exact=False):
        buf = ''
        if not isinstance(field, (list, tuple)):
            field = [field]
        
        for name in field:
            ff = self.xs.getField(name)
            if ff.type is not XSFieldMeta.TYPE_STRING:
                raise XSException("Field `name' cann't be used for facets search, can only be string type")
            buf += chr(ff.vno)
        cmd = {'cmd': CMD_SEARCH_SET_FACETS, 'buf': buf}
        cmd['arg1'] = 1 if exact is True else 0
        self.execCommand(cmd)
        return self
    
    def getFacets(self, field=None):
        if field is None:
            return self._facets
        return self._facets[field] if field in self._facets else {}

    def setScwsMulti(self, level):
        level = int(level)
        if level >= 0 and level < 16:
            cmd = {'cmd': CMD_SEARCH_SCWS_SET, 'arg1': CMD_SCWS_SET_MULTI, 'arg2': level}
            self.execCommand(cmd)
        return self
    
    def setLimit(self, limit, offset=0):
        self._limit = int(limit)
        self._offset = int(offset)
        return self
    
    def setDb(self, name):
        name = str(name) if name else ''
        self.execCommand({'cmd': CMD_SEARCH_SET_DB, 'buf': name})
        self._lastDb = self._curDb
        self._lastDbs = self._curDbs
        self._curDb = name
        self._curDbs = []
        return self
    
    def addDb(self, name):
        name = str(name)
        self.execCommand({'cmd': CMD_SEARCH_ADD_DB, 'buf': name})
        self._curDbs.append(name)
        return self
    
    def markResetScheme(self):
        self._resetScheme = True

    def terms(self, query=None, convert=True):
        query = '' if query is None else self.preQueryString(query)
        if query == '' and self._terms is not None:
            ret = self._terms
        else:
            cmd = XSCommand(CMD_QUERY_GET_TERMS, 0, self._defaultOp, query)
            res = self.execCommand(cmd, CMD_OK_QUERY_TERMS)
            ret = []
            tmps = res.buf.split(' ')
            for tmp in tmps:
                if not tmp or ':' in tmp:
                    continue
                ret.append(tmp)
            if not query:
                self._terms = ret
        return XS.convert(ret, self._charset, 'UTF-8') if convert else ret

    def count(self, query=None):
        query = '' if query is None else self.preQueryString(query)
        if not query and self._count is not None:
            return self._count
        cmd = XSCommand(CMD_SEARCH_GET_TOTAL, 0, self._defaultOp, query)
        res = self.execCommand(cmd, CMD_OK_SEARCH_TOTAL)
        ret = unpack('I', res.buf)
        if not query:
            self._count = ret[0]
        return ret[0]

    def search(self, query=None, saveHighlight=True):
        if self._curDb is not self.LOG_DB and saveHighlight:
            self._highlight = query
        query = '' if query is None else self.preQueryString(query)
        page = pack('II', self._offset, self._limit if self._limit > 0 else self.PAGE_SIZE)
        cmd = XSCommand(CMD_SEARCH_GET_RESULT, 0, self._defaultOp, query, page)
        res = self.execCommand(cmd, CMD_OK_RESULT_BEGIN)
        tmp = unpack('I', res.buf)
        self._lastCount = tmp[0]
        ret = []
        self._facets = {}
        vnoes = self.xs.getScheme().getVnoMap()
        while 1:
            res = self.getRespond()
            if res.cmd == CMD_SEARCH_RESULT_FACETS:
                off = 0
                while (off + 6) < len(res.buf):
                    tmp = unpack('BBI', res.buf[off:off+6])
                    if tmp[0] in vnoes:
                        name = vnoes[tmp[0]]
                        value = res.buf[off+6:off+6+tmp[1]]
                        if name in self._facets:
                            self._facets[name] = {}
                        self._facets[name][value] = tmp[2]
                    off += tmp[1] + 6
            elif res.cmd == CMD_SEARCH_RESULT_DOC:
                doc = XSDocument(res.buf, self._charset)
                ret.append(doc)
            elif res.cmd == CMD_SEARCH_RESULT_FIELD:
                try:
                    name = vnoes[res.arg] if res.arg in vnoes else res.arg
                    doc.setField(name, res.buf)
                except NameError:
                    pass
            elif res.cmd == CMD_SEARCH_RESULT_MATCHED:
                try:
                    doc.setField('matched', explode(' ', res.buf), True)
                except NameError:
                    pass
            elif res.cmd == CMD_OK and res.arg == CMD_OK_RESULT_END:
                break
            else:
                msg = 'Unexpected respond in search {CMD:' + res.cmd + ', ARG:' + res.arg + '}'
                raise XSException(msg)
        if not query:
            self._count = self._lastCount
            if self._curDb is not self.LOG_DB:
                self.logQuery()
                if saveHighlight:
                    self.initHighlight()
        self._limit = self._offset = 0
        return ret

    def getLastCount(self):
        return self._lastCount

    def getDbTotal(self):
        cmd = XSCommand(CMD_SEARCH_DB_TOTAL)
        res = self.execCommand(cmd, CMD_OK_DB_TOTAL)
        return unpack('I', res.buf)[0]

    def getHotQuery(self, limit=6, _type='total'):
        ret = OrderedDict()
        limit = max(1, min(50, int(limit)))
        self.xs.setScheme(XSFieldScheme.logger())
        try:
            self.setDb(self.LOG_DB).setLimit(limit)
            if _type != 'lastnum' and _type != 'currnum':
                _type = 'total'
            result = self.search(_type + ':1')
            for doc in result:
                body = doc.body
                ret[body] = doc.f(_type)
            self.restoreDb()
        except XSException as e:
            if e.getCode() != CMD_ERR_XAPIAN:
                raise XSException(*e.args)
        self.xs.restoreScheme()
        return ret

    def getRelatedQuery(self, query=None, limit=6):
        ret = []
        limit = max(1, min(20, int(limit)))
        if query is None:
            query = self.cleanFieldQuery(self._query)
        if not query or ':' in query:
            return ret
        op = self._defaultOp
        self.xs.setScheme(XSFieldScheme.logger())
        try:
            result = self.setDb(self.LOG_DB).setFuzzy().setLimit(limit + 1).search(query)
            for doc in result:
                doc.setCharset(self._charset)
                body = doc.body
                if body.lower() != query.lower():
                    continue
                ret.append(body)
                if len(ret) == limit:
                    break
        except XSException as e:
            if e.getCode() != CMD_ERR_XAPIAN:
                raise XSException(*e.args)
        self.restoreDb()
        self.xs.restoreScheme()
        self._defaultOp = op
        return ret

    def getExpandedQuery(self, query, limit=10):
        ret = []
        limit = max(1, min(20, int(limit)))
        try:
            buf = XS.convert(query, 'UTF-8', self._charset)
            cmd = {'cmd': CMD_QUERY_GET_EXPANDED, 'arg1': limit, 'buf': buf}
            res = self.execCommand(cmd, CMD_OK_RESULT_BEGIN)
            while 1:
                res = self.getRespond()
                if res.cmd == CMD_SEARCH_RESULT_FIELD:
                    ret.append(XS.convert(res.buf, self._charset, 'UTF-8'))
                elif res.cmd == CMD_OK and res.arg == CMD_OK_RESULT_END:
                    break
                else:
                    msg = 'Unexpected respond in search {CMD: %s , ARG: %s }' % (res.cmd, res.arg)
                    raise XSException(msg)
        except XSException as e:
            if e.getCode() != CMD_ERR_XAPIAN:
                raise XSException(e)
        return ret

    def getCorrectedQuery(self, query=None):
        ret = []
        try:
            if query is None:
                if self._count > 0 and self._count > ceil(self.getDbTotal() * 0.001):
                    return ret
                query = self.cleanFieldQuery(self._query)
            if not query or ':' in query:
                return ret
            buf = XS.convert(query, 'UTF-8', self._charset)
            cmd = {'cmd': CMD_QUERY_GET_CORRECTED, 'buf': buf}
            res = self.execCommand(cmd, CMD_OK_QUERY_CORRECTED)
            if res.buf:
                ret = XS.convert(res.buf, self._charset, 'UTF-8').split("\n")
        except XSException as e:
            if e.getCode() != CMD_ERR_XAPIAN:
                raise XSException(*e.args)
        return ret

    def addSearchLog(self, query, wdf=1):
        cmd = {'cmd': CMD_SEARCH_ADD_LOG, 'buf': query}
        if wdf > 1:
            cmd['buf1'] = pack('i', wdf)
        self.execCommand(cmd, CMD_OK_LOGGED)

    def highlight(self, value, strtr=False):
        if not value:
            return value
    
        if not isinstance(self._highlight, dict):
            self.initHighlight()
    
        if 'pattern' in self._highlight:
            value = re.sub(self._highlight['pattern'], self._highlight['replace'], value)
    
        if 'pairs' in self._highlight:
            pks = self._highlight['pairs'].keys()
            pvs = self._highlight['pairs'].values()
            if strtr:
                value = value.translate(string.maketrans(pks, pvs))
            else:
                i = 0
                for pk in pks:
                    value = value.replace(pk, pvs[i])
                    i += 1
        return value

    def logQuery(self, query=None):
        if self.isRobotAgent():
            return
        if query != '' and query is not None:
            terms = self.terms(query, False)
        else:
            query = self._query
            if not self._lastCount or (self._defaultOp == CMD_QUERY_OP_OR and query.find(' ') != -1) \
                or query.find(' OR ') != -1 or query.find(' NOT ') != -1 or query.find(' XOR ') != -1:
                return
            terms = self.terms(None, False)
        log = ''
        pos = _max = 0
        for term in terms:
            pos1 = (pos - 3) if (pos > 3 and len(term) == 6) else pos
            pos2 = query.find(term, pos1)
            if pos2 == -1:
                continue
            if pos2 == pos:
                log += term
            elif pos2 < pos:
                log += term[3:]
            else:
                _max += 1
                if _max > 3 or len(log) > 42:
                    break
                log += ' ' + term
            pos = pos2 + len(term)
        log = log.strip()
        if len(log) < 2 or (len(log) == 3 and ord(log[0]) > 0x80):
            return
        self.addSearchLog(log)

    def clearQuery(self):
        cmd = XSCommand(CMD_QUERY_INIT)
        if self._resetScheme is True:
            cmd.arg1 = 1
            self._prefix = {}
            self._fieldSet = False
            self._resetScheme = False
    
        self.execCommand(cmd)
        self._query = self._count = self._terms = None

    def addQueryString(self, query, addOp=CMD_QUERY_OP_AND, scale=1):
        query = self.preQueryString(query)
        bscale = pack('n', int(scale * 100)) if (scale > 0 and scale != 1)  else ''
        cmd = XSCommand(CMD_QUERY_PARSE, addOp, self._defaultOp, query, bscale)
        self.execCommand(cmd)
        return query

    def addQueryTerm(self, field, term, addOp=CMD_QUERY_OP_AND, scale=1):
        term = strtolower(term)
        term = XS.convert(term, 'UTF-8', self._charset)
        bscale = pack('n', int(scale * 100)) if (scale > 0 and scale != 1)  else ''
        vno = XSFieldScheme.MIXED_VNO if field is None else self.xs.getField(field, True).vno
        cmd = XSCommand(CMD_QUERY_TERM, addOp, vno, term, bscale)
        self.execCommand(cmd)
        return self

    def restoreDb(self):
        db = self._lastDb
        dbs = self._lastDbs
        self.setDb(db)
        for name in dbs:
            self.addDb(name)
    
    def preQueryString(self, query):
        query = query.strip()
        if self._resetScheme is True:
            self.clearQuery()
        self.initSpecialField()
        newQuery = ''
        parts = re.split('[ \t\r\n]+', query)
        for part in parts:
            if part == '':
                continue
            if newQuery != '':
                newQuery += ' '
            pos = part.find(':', 1)
            if pos != -1:
                for i in xrange(0, pos):
                    if part[i] not in '+-~(':
                        break
                name = part[i:pos - i]
                field = self.xs.getField(name, False)
                if field is not False and field.vno != XSFieldScheme.MIXED_VNO:
                    self.regQueryPrefix(name)
                    if field.hasCustomTokenizer():
                        prefix = part[0:i] if i > 0 else ''
                        suffix = ''
                        value = part[pos + 1:]
                        if value[-1:1] == ')':
                            suffix = ')'
                            value = value[0:-1]
                        terms = []
                        tokens = field.getCustomTokenizer().getTokens(value)
                        for term in tokens:
                            terms.append(term.lower())
                        terms = list(set(terms))
                        newQuery += prefix + name + ':' + (' ' + name + ':').join(terms) + suffix
                    elif part[pos + 1:1] != '(' and re.search('[\x81-\xfe]', part):
                        newQuery += part[0:pos + 1] + '(' + part[pos + 1:] + ')'
                    else:
                        newQuery += part
                    continue
            if len(part) > 1 and (part[0] == '+' or part[0] == '-') and part[1] != '(' \
                    and re.search('[\x81-\xfe]', part):
                newQuery += part[0:1] + '(' + part[1:] + ')'
                continue
            newQuery += part
        return XS.convert(newQuery, 'UTF-8', self._charset)

    def regQueryPrefix(self, name):
        field = self.xs.getField(name, False)
        if name not in self._prefix and field and (field.vno != XSFieldScheme.MIXED_VNO):
            _type = CMD_PREFIX_BOOLEAN if field.isBoolIndex() else CMD_PREFIX_NORMAL
            cmd = XSCommand(CMD_QUERY_PREFIX, _type, field.vno, name)
            self.execCommand(cmd)
            self._prefix[name] = True
    
    def initSpecialField(self):
        if self._fieldSet is True:
            return
        _fields = self.xs.getAllFields()
        for k in _fields:
            field = _fields[k]
            if field.cutlen != 0:
                flen = min(127, math.ceil(field.cutlen / 10))
                cmd = XSCommand(CMD_SEARCH_SET_CUT, flen, field.vno)
                self.execCommand(cmd)
            if field.isNumeric():
                cmd = XSCommand(CMD_SEARCH_SET_NUMERIC, 0, field.vno)
                self.execCommand(cmd)
        self._fieldSet = True

    def cleanFieldQuery(self, query):
        query = strtr(query, {' AND ': ' ', ' OR ': ' '})
        if query.find(':') != -1:
            regex = '(^|\s)([0-9A-Za-z_\.-]+):([^\s]+)'
            return re.sub(regex, self.cleanFieldCallback, query)
        return query

    def cleanFieldCallback(self, match):
        field = self.xs.getField(match[2], False)
        if field is False:
            return match[0]
        if field.isBoolIndex():
            return ''
        if match[3][0:1] == '(' and match[3][-1:1] == ')':
            match[3] = match[3][1:-1]
        return match[1] + match[3]

    def initHighlight(self):
        terms = []
        tmps = self.terms(self._highlight, False)
        for i in xrange(0, len(tmps)):
            if len(tmps[i]) != 6 or ord(tmps[i][0:1]) < 0xc0:
                terms.append(XS.convert(tmps[i], self._charset, 'UTF-8'))
                continue
            for j in (i+1, len(tmps)):
                if len(tmps[j]) != 6 or tmps[j][0:3] != tmps[j - 1][3:6]:
                    break 
            k = j - i     
            if k == 1:
                terms.append(XS.convert(tmps[i], self._charset, 'UTF-8'))
            else:
                i = j - 1
                while k:
                    k -= 1
                    j -= 1
                    if k & 1:
                        terms.append(XS.convert(tmps[j - 1][0:3] . tmps[j], self._charset, 'UTF-8'))
                    terms.append(XS.convert(tmps[j], self._charset, 'UTF-8'))
        pattern = []
        replace = []
        pairs = {}
        for term in terms:
            if re.match('[a-zA-Z]', term):
                pairs[term] = '<em>' + term + '</em>'
            else:
                pattern.append((term.replace('+', '\\+').replace('/', '\\/'), re.I))
                replace.append('<em>0</em>')
        self._highlight = {}
        if len(pairs) > 0:
            self._highlight['pairs'] = pairs
        if len(pattern) > 0:
            self._highlight['pattern'] = pattern
            self._highlight['replace'] = replace
    
    def formatValueRange(self, match):
        field = self.xs.getField(int(match[2]), False)
        if field is False:
            return match[0]
        val1 = val2 = '~'
        try:
            val2 = self.xapianUnserialise(match[4]) if field.isNumeric() else match[4]
        except IndexError:
            pass
        if match[1] == 'VALUE_LE':
            val2 = self.xapianUnserialise(match[3]) if field.isNumeric() else match[3]
        else:
            val1 = self.xapianUnserialise(match[3]) if field.isNumeric() else match[3]
        return field.name + ':[' + val1 + ',' + val2 + ']'

    def xapianUnserialise(self, value):
        if value == "\x80":
            return 0.0
        if value == ("\xff" * 9):
            return INF
        if value == '':
            return -INF
        i = 0
        c = ord(value[0])
        c ^= (c & 0xc0) >> 1
        negative = 1 if not (c & 0x80) else 0
        exponent_negative = 1 if (c & 0x40) else 0
        explen = 1 if not (c & 0x20) else 0
        exponent = c & 0x1f
        if not explen:
            exponent >>= 2
            if negative ^ exponent_negative:
                exponent ^= 0x07
        else:
            c = ord(value[++i])
            exponent <<= 6
            exponent |= (c >> 2)
            if negative ^ exponent_negative:
                exponent &= 0x07ff

        word1 = (c & 0x03) << 24
        word1 |= ord(value[++i]) << 16
        word1 |= ord(value[++i]) << 8
        word1 |= ord(value[++i])
        word2 = 0
        if i < len(value):
            word2 = ord(value[++i]) << 24
            word2 |= ord(value[++i]) << 16
            word2 |= ord(value[++i]) << 8
            word2 |= ord(value[++i])
    
        if not negative:
            word1 |= 1 << 26
        else:
            word1 = 0 - word1
            if word2 != 0:
                ++word1
            word2 = 0 - word2
            word1 &= 0x03ffffff
        mantissa = 0
        if word2:
            mantissa = word2 / 4294967296.0 // 1<<32
        mantissa += word1
        mantissa /= 1 << (26 if negative == 1 else 27)
        if exponent_negative:
            exponent = 0 - exponent
        exponent += 8
        if negative:
            mantissa = 0 - mantissa
        return round(mantissa * pow(2, exponent), 2)

    def isRobotAgent(self):
        return False


class XSCommand(XSComponent):

    cmd = CMD_NONE
    arg1 = 0
    arg2 = 0
    buf = ''
    buf1 = ''

    def __init__(self, cmd, arg1=0, arg2=0, buf='', buf1=''):
        if isinstance(cmd, dict):
            for key in cmd:
                value = cmd[key]
                if key == 'arg' or hasattr(self, key):
                    setattr(self, key, value)
        else:
            self.cmd = cmd
            self.arg1 = arg1
            self.arg2 = arg2
            self.buf = buf.encode('utf-8') if isinstance(buf, unicode) else buf
            self.buf1 = buf1
    
    def __str__(self):
        if len(self.buf1) > 0xff:
            self.buf1 = self.buf1[0:0xff]
        return pack('BBBBI', self.cmd, self.arg1, self.arg2, len(self.buf1), len(self.buf)) + self.buf + self.buf1

    def getArg(self):
        return self.arg2 | (self.arg1 << 8)

    def setArg(self, arg):
        self.arg1 = (arg >> 8) & 0xff
        self.arg2 = arg & 0xff


class XSTokenizer:
    DFL = 0
    def getTokens(self, value, doc=None):
        raise NotImplementedError('This method is not implemented')


class XSTokenizerNone(XSTokenizer):
    def getTokens(self, value, doc=None):
        return []


class XSTokenizerFull(XSTokenizer):
    def getTokens(self, value, doc=None):
        return [value]


class XSTokenizerSplit(XSTokenizer):
    _arg = ' '

    def __init__(self, arg=None):
        if arg is not None and arg != '':
            self._arg = arg
    
    def getTokens(self, value, doc=None):
        if len(self._arg) > 2 and self._arg[0:1] == '/' and self._arg[-1:1] == '/':
            return preg_split(self._arg, value)
        return value.split(self._arg)


class XSTokenizerXlen(XSTokenizer):
    _arg = 2

    def __init__(self, arg=None):
        if arg is not None and arg != '':
            self._arg = int(arg)
            if self._arg < 1 or self.arg > 255:
                raise XSException('Invalid argument for %s : %s' % (self.__class__.__name__, arg))
        
    def getTokens(self, value, doc=None):
        terms = []
        for i in xrange(0, len(value), self._arg):
            terms.append(value[i:self._arg])
        return terms

class XSTokenizerXstep(XSTokenizer):
    _arg = 2

    def __init__(self, arg=None):
        if arg is not None and arg != '':
            self._arg = int(arg)
            if self._arg < 1 or self._arg > 255:
                raise XSException('Invalid argument for %s : %s' % (self.__class__.__name__, arg))
        
    def getTokens(value, doc=None):
        terms = []
        i = self._arg
        while 1:
            terms.append(value[0:i])
            if i >= len(value):
                break
            i += self._arg
        return terms


class XSTokenizerScws(XSTokenizer):

    MULTI_MASK = 15
    _charset = None
    _setting = {}
    _server = None

    def __init__(self, arg=None):
        if self._server is None:
            xs = XS.getLastXS()
            if xs is None:
                raise XSException('An XS instance should be created before using %s' % 
                                  (self.__class__.__name__, ))
            self._server = xs.getScwsServer()
            self._server.setTimeout(0)
            self._charset = xs.getDefaultCharset()
            if 'SCWS_MULTI_NONE' not in globals():
                SCWS_MULTI_NONE = 0
                SCWS_MULTI_SHORT = 1
                SCWS_MULTI_DUALITY = 2
                SCWS_MULTI_ZMAIN = 4
                SCWS_MULTI_ZALL = 8
            if 'SCWS_XDICT_XDB' not in globals():
                SCWS_XDICT_XDB = 1
                SCWS_XDICT_MEM = 2
                SCWS_XDICT_TXT = 4
        if arg is not None and arg != '':
            self.setMulti(arg)
    
    def getTokens(self, value, doc=None):
        tokens = []
        self.setIgnore(True)
        _charset = self._charset
        self._charset = 'UTF-8'
        words = self.getResult(value)
        for word in words:
            tokens.append(word['word'])
        self._charset = _charset
        return tokens

    def setCharset(self, charset):
        self._charset = charset.upper()
        if self._charset == 'UTF8':
            self._charset = 'UTF-8'
        return self

    def setIgnore(self, yes=True):
        self._setting['ignore'] = XSCommand(CMD_SEARCH_SCWS_SET, CMD_SCWS_SET_IGNORE,
                                            0 if not yes else 1)
        return self

    def setMulti(self, mode=3):
        mode = int(mode) & self.MULTI_MASK
        self._setting['multi'] = XSCommand(CMD_SEARCH_SCWS_SET, CMD_SCWS_SET_MULTI, mode)
        return self

    def setDict(self, fpath, mode=None):
        if not isinstance(mode, int):
            mode = SCWS_XDICT_TXT if '.txt' in fpath.lower() else SCWS_XDICT_XDB
        self._setting['set_dict'] = XSCommand(CMD_SEARCH_SCWS_SET, CMD_SCWS_SET_DICT, mode, fpath)
        del self._setting['add_dict']
        return self

    def addDict(self, fpath, mode=None):
        if not isinstance(mode, int):
            mode = SCWS_XDICT_TXT if '.txt' in fpath.lower() else SCWS_XDICT_XDB
        if 'add_dict' not in self._setting:
            self._setting['add_dict'] = []
        self._setting['add_dict'].append(XSCommand(CMD_SEARCH_SCWS_SET, CMD_SCWS_ADD_DICT, mode, fpath))
        return self

    def setDuality(self, yes=True):
        self._setting['duality'] = XSCommand(CMD_SEARCH_SCWS_SET, CMD_SCWS_SET_DUALITY,
                                             0 if not yes else 1)
        return self

    def getVersion(self):
        cmd = XSCommand(CMD_SEARCH_SCWS_GET, CMD_SCWS_GET_VERSION)
        res = self._server.execCommand(cmd, CMD_OK_INFO)
        return res.buf

    def getResult(self, text):
        words = []
        text = sself._applySetting(text)
        cmd = XSCommand(CMD_SEARCH_SCWS_GET, CMD_SCWS_GET_RESULT, 0, text)
        res = self._server.execCommand(cmd, CMD_OK_SCWS_RESULT)
        while res.buf != '':
            _off, _attr, _word = unpack('Iaa', res.buf)
            tmp = {
                'off': _off,
                'attr': _attr,
                'word': _word,
            }
            tmp['word'] = XS.convert(tmp['word'], self._charset, 'UTF-8')
            words.append(tmp)
            res = self._server.getRespond()
        return words

    def getTops(self, text, limit=10, xattr=''):
        words = []
        text = sself._applySetting(text)
        cmd = XSCommand(CMD_SEARCH_SCWS_GET, CMD_SCWS_GET_TOPS, limit, text, xattr)
        res = self._server.execCommand(cmd, CMD_OK_SCWS_TOPS)
        while res.buf != '':
            _off, _attr, _word = unpack('Iaa', res.buf)
            tmp = {
                'off': _off,
                'attr': _attr,
                'word': _word,
            }
            tmp['word'] = XS.convert(tmp['word'], self._charset, 'UTF-8')
            words.append(tmp)
            res = self._server.getRespond()
        return words

    def hasWord(self, text, xattr):
        text = sself._applySetting(text)
        cmd = XSCommand(CMD_SEARCH_SCWS_GET, CMD_SCWS_HAS_WORD, 0, text, xattr)
        res = self._server.execCommand(cmd, CMD_OK_INFO)
        return res.buf is 'OK'

    def _applySetting(self, text):
        self._server.reopen()
        for key in self._setting:
            cmd = self._setting[key]
            if isinstance(cmd, dict):
                for _cmd in cmd:
                    self._server.execCommand(_cmd)
            else:
                self._server.execCommand(cmd)
        return XS.convert(text, 'UTF-8', self._charset)
