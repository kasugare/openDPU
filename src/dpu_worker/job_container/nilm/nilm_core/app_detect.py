#!/usr/bin/env python
# -*- coding: utf-8 -*-

from app_label import SimpleRuleAppLabel
import subprocess
import traceback
import time
import json
import sys

"""
BaseAppDetect

Extract meta-info of appliances from given data

"""
CORE_DIR = '/home/nilm/NilmMultiCluster/src/nilm_worker/job_container/nilm/nilm_core/resources'

class BaseAppDetect:
	def __init__(self, _logger):
		self._logger = _logger
		self.app_info = None
		self.existed_meta = None
		self.datafile = None
		self.r_output = None
		self.pid = None
		self.hz = None

	def set_data(self, datafile, hz):
		self._logger.info("# Set nilm data, file path : %s, freq : %dhz" %(datafile, hz))
		self.datafile = datafile
		self.hz = hz

	def setExistedMeta(self, existedMeta):
		self._logger.info("# Set existed nilm meta data : %s" %(str(existedMeta)))
		self.existed_meta = json.dumps(existedMeta)

	def detect_apps(self):
		raise NotImplementedError

	def get_app_info(self,json_string=False):
		if json_string:
			return json.dumps(self.app_info)
		else:
			return self.app_info
	def getAppInfo(self):
		return json.dumps(self.app_info)

class AppDetectR(BaseAppDetect):
	def __init__(self, _logger):
		BaseAppDetect.__init__(self, _logger)
		self._logger = _logger

	def detect_apps(self):
		try:
			if not self.existed_meta:
				self._logger.info("# Do analyze NILM meta detection")
				if self.hz == 1:
					cmd = "echo '" + self.datafile + "' | Rscript " + CORE_DIR + "/app_detector_stdio_1hz.R"
				elif self.hz == 15:
					cmd = "echo '" + self.datafile + "' | Rscript " + CORE_DIR + "/app_detector_stdio_15hz.R"
				else:
					self._logger.warn("- not allow device frequence. freq : %d hz" %(self.hz))
			else:
				self._logger.info("# Do analyze NILM meta update detection")
				if self.hz == 15:
					# cmd = "echo '" + self.datafile + "_encored_" + self.existed_meta + "' | Rscript " + CORE_DIR + "/app_detector_stdio_15hz.R"
					cmd = "echo '" + self.datafile + "' | Rscript " + CORE_DIR + "/app_detector_stdio_15hz.R"
				else:
					self._logger.warn("- not allow device frequence. freq : %d hz" %(self.hz))

			self._logger.debug("- R command : %s" %cmd)
			self.pid = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
			r_output = self.pid.stdout.read()

			if not r_output:
				r_output = None

			self.r_output = r_output
			self.app_info = json.loads(r_output)
			# self._logger.debug("- R result, nilm meta : %s" %(str(self.app_info)))

			app_label = SimpleRuleAppLabel(self._logger)
			del_app_infos = []
			for k in self.app_info:
				try:
					one_app = self.app_info[k]
					self.app_info[k]['encored_appliance_type'] = app_label.find_app_type(self.app_info[k])
				except Exception, e:
					self._logger.exception(e)
					del_app_infos.append(k)

			if del_app_infos:
				for k in del_app_infos:
					del self.app_info[k]

			# self._logger.debug("- appliance info : %s" %(str(self.app_info)))
			return True
		except Exception, e:
			self._logger.exception(e)
			return False

