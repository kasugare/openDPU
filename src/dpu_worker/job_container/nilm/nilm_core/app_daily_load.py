#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback
import subprocess
import time
import json
import sys


"""
BaseAppDailyLoad

Estimate daily load of a given appliance

"""
CORE_DIR = '/home/nilm/NilmMultiCluster/src/nilm_worker/job_container/nilm/nilm_core/resources'

class BaseAppDailyLoad:
	def __init__(self, _logger):
		self._logger = _logger
		self.app_info = None
		self.data = None
		self.usage = None
		self.pid = None
		self.hz = None

	def set_data(self, datafile, hz):
		self._logger.info("# Set nilm data, file path : %s, freq : %dhz" %(datafile, hz))
		self.datafile = datafile
		self.hz = hz

	def set_app_info(self,app_info):
		self.app_info

	def get_usage(self):
		return self.usage

	def do_compute(self):
		raise NotImplementedError

class AppDailyLoadR(BaseAppDailyLoad):
	def __init__(self, _logger):
		BaseAppDailyLoad.__init__(self, _logger)
		self._logger = _logger

	def set_app_info(self, app_info):
		self._logger.info("# Set appliance meta info")
		self.app_info = json.dumps(app_info)

	def do_compute(self):
		self._logger.info("# Do analyze NILM daily usage")
		try:
			if self.hz == 1:
				cmd = "echo '" + self.datafile + "_encored_" + self.app_info + "' | Rscript " + CORE_DIR + "/daily_load_stdio_1hz.R"
			elif self.hz == 15:
				cmd = "echo '" + self.datafile + "_encored_" + self.app_info + "' | Rscript " + CORE_DIR + "/daily_load_stdio_15hz.R"
			else:
				self._logger.warn("- not allow device frequence. freq : %d hz" %(self.hz))
			self._logger.debug("- R command : %s" %cmd)
			self.pid = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
			r_output = self.pid.stdout.read()

			# self._logger.debug("- R result, raw daily usage : %s" %(r_output))
			self.usage = json.loads(r_output)
			# self._logger.debug("- R result, daily usage : %s" %(str(self.usage)))
			return True

		except Exception, e:
			self._logger.exception(e)
			self._logger.error("Unexpected error: %s" %str(sys.exc_info()[0]))
			# raise SystemExit
