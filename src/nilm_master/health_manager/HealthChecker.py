#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_system import getMaxRetryCount
from protocol.message_pool.MessageGenerator import genReqHB
import time

class HealthChecker:
	def __init__(self, logger, context):
		self._logger = logger
		self._context = context
		self.macRetry = getMaxRetryCount()
		self.retryCnt = 0

	def run(self, interval = 10):
		if self._context.doTerminsation:
			return

		while self._context.isRunnable:
			if self._context._sender.sendMsg(genReqHB()):
				self._context.isRunnable = False
				self.retryCnt = 0
				time.sleep(interval)
			else:
				self._context.doTerminsation = True
				self._context.isRunnable = False
				return
