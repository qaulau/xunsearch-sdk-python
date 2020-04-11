# -*- encoding: utf-8 -*-
# 

import unittest
import xunsearch


class DiscuzTest(unittest.TestCase):

	def setUp(self):
		self.xs = xunsearch.XS('discuz.ini')

	def test_config(self):
		self.assertEqual(self.xs.getName(), 'discuz')
		self.assertEqual(self.xs.getDefaultCharset(), 'GBK')
		self.assertEqual(len(self.xs.getAllFields()), 9)


if __name__ == '__main__':
    unittest.main()