#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common_utility import getHourlyDatetime, getDailyDatetime, getPeriodTS
from optparse import OptionParser
from datetime import datetime
import traceback
import copy
import sys
import os

class UserOptionParser:
	def __init__(self):
		self._options()

	def _options(self):
		usage = """usage: %prog [options] arg1,arg2 [options] arg1\n$ python DpuClient.py -v -p 2015-01-01_00:00:00 2015-01-02_23:59:59 -s 1 -D edm3_090"""

		parser = OptionParser(usage = usage)
		parser.add_option("--admin",
			action="store_true",
			default=False,
			dest="adminMode",
			help="""use this option it will be changed admin mode for DPU management
			[use] --admin                           \n
			[options] --admin                       \n
			[default] %default""")

		options, args = parser.parse_args()
		self._vaildOptions(options, args)

	def _vaildOptions(self, options, args):
		optVaildator = OptionValidator(options, args)
		self.userOptions = optVaildator.doCheckOptions()

	def getUserOption(self):
		print "-" * 50
		for optKey in self.userOptions:
			print '- %s : %s' %(optKey.ljust(15), str(self.userOptions[optKey]))
		print "-" * 50

		return self.userOptions


class OptionValidator:
	def __init__(self, options, args):
		self._openDpuOptions = self._setOpenDpuOptions(options, args)

	def _setOpenDpuOptions(self, options, args):
		if options.adminMode != None:
			openDpuOptions['adminMode'] = options.adminMode
		return openDpuOptions
