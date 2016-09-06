#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger

PROCESS_NAME = 'DEPLOY_MANAGER'

class DeployManager:
	def __init__(self, deployQ):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('# process start : %s' %PROCESS_NAME)
		self._reqQ = deployQ['reqQ']
		self._routerQ = deployQ['routerQ']

	def __del__(self):
		self._logger.warn('@ terminate process : %s' %PROCESS_NAME)

	def doProcess(self):
		try:
			while True:
				reqMsg = self._routerQ.get()
				print "[Q] DeployManager :", reqMsg
				print reqMsg
		except KeyboardInterrupt, e:
			pass