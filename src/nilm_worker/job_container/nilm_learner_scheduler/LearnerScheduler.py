#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from SchedulingHandler import SchedulingHandler

PROCESS_NAME = 'LEARNER_SCHEDULER'

class LearnerScheduler:
	def __init__(self, logger=None, debugMode='DEBUG'):
		if logger:
			self._logger = logger
		else:
			self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.setLevel(debugMode)

	def doProcess(self):
		self._logger.info("#######################################")
		self._logger.info("### DO Process Nilm Learn Scheduler ###")
		self._logger.info("#######################################")

		SchedulingHandler(self._logger).doProcess()

if __name__ == '__main__':
	loadBalancer = LearnerScheduler()
	loadBalancer.doProcess()
