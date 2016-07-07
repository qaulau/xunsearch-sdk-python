# -*- encoding: utf-8 -*-


__author__ = 'qaulau'
__doc__ = """\
自定义分词 Chip1step
"""

from xunsearch import XSTokenizer
from xunsearch import XSException


class XSTokenizerChipstep(XSTokenizer):

	__arg = 2

	def __init__(self, arg=None):
		if arg is not None and arg != '':
			self.__arg = int(arg)
			if self.__arg < 1 or self.__arg > 255:
				raise XSException('Invalid argument for %s: %s' % (self.__class__.__name__, arg))

	def getTokens(self, value, doc=None):
		terms = []
		if value and '€€' in value:
			value_list = value.split('€€')
			for vo in value_list:
				terms.append(self.getTokens(vo))
			return terms
		i = self.__arg
		if not isinstance(value, unicode):
			value = value.decode('utf-8')
		slen = len(value)
		for start in xrange(0, slen):
			while 1:
				tmp = value[start:start+i]
				if len(tmp) >= 2:
					terms.append([tmp] if start == 0 else tmp)
				if i >= (slen - start):
					break
				i += self.__arg;
			i = self.__arg
			start += 1
		return terms