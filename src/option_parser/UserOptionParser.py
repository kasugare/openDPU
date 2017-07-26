#!/usr/bin/env python
# -*- coding: utf-8 -*-

from OptionValidator import OptionValidator
from optparse import OptionParser
from datetime import datetime
import traceback
import copy
import sys
import os

class UserOptionParser:
	def __init__(self, logger):
		self._logger = logger
		self._options()

	def _options(self):
		self._logger.info("# Set user options")
		usage = """usage: %prog [options] arg1,arg2 [options] arg1
$ python NilmCluster.py -v -p 2015-01-01_00:00:00 2015-01-02_23:59:59 -s 1 -D edm3_090"""

		parser = OptionParser(usage = usage)
		parser.add_option("-s", "--sids",
			action="store",
			type="str",
			dest="sids",
			nargs=1,
			default="*",
			help="""generated data by a site id. It will be create only one site id.
			[use] -s 1 or  --sid=1
			[options] -s 1,2,3 or not
			[default: %default]""")

		parser.add_option("-p", "--period",
			action="store",
			dest="period",
			type="str",
			nargs=2,
			default=None,
			help="""[optional] data collecting period
			[use] -p 2015-07-01_00:00:00 2015-07-01_23:59:59""")

		parser.add_option("-P", "--USAGE_PERIOD",
			action="store",
			dest="usagePeriod",
			type="str",
			default="daily",
			help="""[optional] nilm analysis period
			[use] -P hourly or -P daily
			[default: %default]""")

		parser.add_option("-D", "--device_protocol",
			action="store",
			dest="deviceProtocol",
			default=None,
			help="""select protocol of the device on tajo
			[use] -D edm1_090,edm3_090
			[options] edm1_090, edm3_090, edm3_100
			[default: %default]""")

		parser.add_option("-j", "--job",
			action="store",
			dest="job",
			default=None,
			help="""set processing job
			[use] -j app_search, app_usage or candidate
			[options] app_search, app_usage
			[default: %default]""")

		parser.add_option("-g", "--groups",
			action="store",
			dest="groups",
			default=None,
			help="""set processing groups
			[use] -g closedBeta or openBeta or closedBeta,openBeta,EncoredTech
			[options] closedBeta, openBeta, EncoredTech or ETC
			[default: %default]""")

		parser.add_option("--DEVICE_META",
			action="store_true",
			default=False,
			dest="deviceMeta",
			help="""search device meta as the device frequence on raw data
			[use] --DEVICE_META
			[default: %default]""")

		parser.add_option("--PROCESS",
			action="store",
			type="int",
			dest="numOfProcess",
			default=1,
			help="""set number of processing analytics processors
			[use] --PROCESS 4
			[options] number of processors(integer)
			[default: %default]""")

		parser.add_option("--FILE_SAVE",
			action="store_true",
			default=False,
			dest="fileSave",
			help="""use this option, raw data will be saved in directory
			[use] --FILE_SAVE
			[default: %default]""")

		parser.add_option("--NOT_IN_DB",
			action="store_false",
			default=True,
			dest="injection",
			help="""use this option results of the nilm will be saved in DB
			[use] --NOT_IN_DB or None
			[default: %default]""")

		parser.add_option("--REMOVE",
			action="store",
			default=None,
			dest="remove",
			help="""use this option remove the nilm data in DB
			[use] --REMOVE=meta,usage ...
			[options] appliance, meta, router, usage, all(*)
			[default: %default]""")

		parser.add_option("--HAS_FILE",
			action="store_true",
			default=True,
			dest="hasFile",
			help="""use this option if it have been processed,
			it will not be searched by tajo for collecting raw data.
			[use] --HAS_FILE
			[options] --HAS_FILE or None
			[default: %default""")

		parser.add_option("--RAW_DATA",
			action="store_true",
			default=False,
			dest="onlyRawdata",
			help="""use this option it will be get only NILM_raw data.
			[use] --RAW_DATA
			[options] --RAW_DATA or None
			[default: %default"""
			)

		parser.add_option("--DEBUG",
			action="store_true",
			default=False,
			dest="debugMode",
			help="""use this option if it have been processed,
			it will operate in DEBUG mode.
			[use] --DEBUG
			[options] --DEBUG or None
			[default: %default""")

		options, args = parser.parse_args()
		self._vaildOptions(options, args)

	def _vaildOptions(self, options, args):
		optVaildator = OptionValidator(self._logger, options, args)
		self.orderSheet = optVaildator.doCheckOptions()

	def getUserOption(self):
		self._logger.info("# User option infos")
		self._logger.info("-" * 50)
		for optKey in self.orderSheet:
			self._logger.info('- %s : %s' %(optKey.ljust(15), str(self.orderSheet[optKey])))
		self._logger.info("-" * 50)

		return self.orderSheet