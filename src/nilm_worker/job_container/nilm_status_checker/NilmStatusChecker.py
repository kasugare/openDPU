#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from manager.SiteStatusManager import SiteStatusManager

PROCESS_NAME = 'NILM_STATUS'

class NilmStatusChecker:
	def __init__(self, logger=None, debugMode='DEBUG'):
		if logger:
			self._logger = logger
		else:
			self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.setLevel(debugMode)

	def doProcess(self, jobType, message):
		self._logger.info("######################################")
		self._logger.info("### DO Process Nilm Status Checker ###")
		self._logger.info("######################################")

		startTS = message['params']['startTS']
		endTS = message['params']['endTS']
		gids = message['params']['gids']
		SiteStatusManager(self._logger).doProcess(startTS, endTS, gids)
		
