#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from common.util_protocol_parser import parseProtocol

PROCESS_NAME = 'JOB_MANAGER'

class JobManager:
	def __init__(self, jobQ):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('# process start : %s' %PROCESS_NAME)
		self._reqQ = jobQ['reqQ']
		self._routerQ = jobQ['routerQ']

	def __del__(self):
		self._logger.warn('@ terminate process : %s' %PROCESS_NAME)

	def doProcess(self):
		try:
			# self._loadJobs()

			while True:
				reqMsg = self._routerQ.get()
				print "[Q] JobManager(SYS) :", reqMsg
				protocol, statCode = parseProtocol(reqMsg)

		except KeyboardInterrupt, e:
			pass

