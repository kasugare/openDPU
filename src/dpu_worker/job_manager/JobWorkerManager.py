#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger

PROCESS_NAME = 'JOB_WORKER'

class JobWorkerManager:
	def __init__(self, jobQ):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('# process start : %s' %PROCESS_NAME)
		self._reqQ = jobQ['reqQ']
		self._routerQ = jobQ['routerQ']

	def __del__(self):
		self._logger.warn('@ terminate process : %s' %PROCESS_NAME)

	def doProcess(self):
		try:
			self._reqQ.put_nowait("[%s] : message test" %PROCESS_NAME)

			while True:
				reqMsg = self._routerQ.get()
				print "[Q] JobWorkerManager :", reqMsg
		except KeyboardInterrupt, e:
			pass

	def putMessageQueue(self, message):
		self._reqQ.put_nowait(message)
