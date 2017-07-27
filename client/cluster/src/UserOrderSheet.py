#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common_utility import getHourlyDatetime, getDailyDatetime, getPeriodTS
from optparse import OptionParser
from datetime import datetime
import traceback
import copy
import sys
import os

STORAGES = ['FILE', 'HDFS']

class UserOptionParser:
	def __init__(self):
		self._options()

	def _options(self):
		usage = """usage: %prog [options] arg1,arg2 [options] arg1
$ python NilmCluster.py -v -p 2015-01-01_00:00:00 2015-01-02_23:59:59 -s 1 -D edm3_090"""

		parser = OptionParser(usage = usage)
		parser.add_option("-v", "--version",
			action="store_true",
			default=False,
			dest="version",
			help="""get version of the Nilm Cluster
			[use] -v or --version""")

		parser.add_option("-j", "--jobs",
			action="store",
			type="str",
			dest="jobs",
			nargs=1,
			default="etl",
			help="""
				[use] -j etl, or --jobs=etl
				[default: %default]""")

		parser.add_option("-s", "--storage",
			action="store",
			type="str",
			dest="storage",
			nargs=1,
			default="file",
			help="""
				[use] -j file, or --storage=file
				[options] file, hdfs
				[default: %default]""")

		parser.add_option("-p", "--paths",
			action="store",
			type="str",
			dest="paths",
			nargs=1,
			default=None,
			help="""
				[use] -p /data/file1,/data/file2 or --paths=/data/file1,data/file2
				[options] file, hdfs
				[default: %default]""")

		parser.add_option("--RESOURCES",
			action="store",
			dest="workers",
			default=None,
			help="""
				[use] --RESOURCES
				[options] --RESOURCES or None
				[default: %default""")

		parser.add_option("--DEBUG",
			action="store_true",
			default=False,
			dest="debugMode",
			help="""[use] --DEBUG
				[options] --DEBUG or None
				[default: %default""")

		options, args = parser.parse_args()
		self._vaildOptions(options, args)

	def _vaildOptions(self, options, args):
		optVaildator = OptionValidator(options, args)
		self.userOptions = optVaildator.doCheckOptions()

	def getUserOption(self):
		debugMode = self.userOptions.get('debugMode')
		if debugMode:
			print "-" * 50
			for optKey in self.userOptions:
				print '- %s : %s' %(optKey.ljust(15), str(self.userOptions[optKey]))
			print "-" * 50
		return self.userOptions


class OptionValidator:
	def __init__(self, options, args):
		self._userOptions = self._setUserOptions(options, args)

	def _setUserOptions(self, options, args):
		user_options = {}
		if options.version != None:
			user_options['version'] = options.version
		if options.jobs != None:
			user_options['jobs'] = options.jobs
		if options.storage != None:
			user_options['storage'] = options.storage
		if options.paths != None:
			user_options['paths'] = options.paths.split(',')
		if options.workers != None:
			user_options['workers'] = options.workers
		if options.debugMode != None:
			user_options['debugMode'] = options.debugMode
		return user_options

	def doCheckOptions(self):
		userOptions = {}
		userOptions['version'] = self._userOptions.get('version')
		userOptions['workers'] = self._checkWorkerResoures()
		userOptions['debugMode'] = self._userOptions.get('debugMode')

		if not userOptions['workers']:
			userOptions['jobs'] = self._checkJob()
			userOptions['storage'] = self._checkStorage()
			userOptions['paths'] = self._checkStoragePaths()

		return userOptions

	def _checkJob(self):
		jobs = self._userOptions.get('jobs')
		if jobs.upper() == 'ETL':
			return jobs.upper()
		else:
			print "# It only use ETL option"
			sys.exit(1)

	def _checkStorage(self):
		storage = self._userOptions.get('storage')
		if storage.upper() in STORAGES:
			return storage.upper()
		else:
			print "# Wrong storage type. You have to check this option[file or hdfs]"
			sys.exit(1)

	def _checkStoragePaths(self):
		paths = self._userOptions.get('paths')
		if not paths:
			print "# Empty storage paths. You must use a option or more."
			sys.exit(1)
		else:
			return paths

	def _checkWorkerResoures(self):
		workers = self._userOptions.get('workers')
		if "*" == workers:
			workers = "all"
			return workers
		elif "all" == workers:
			return workers
		elif not workers:
			return None
		else:
			print "# Wrong workers resources options"
			sys.exit(1)
