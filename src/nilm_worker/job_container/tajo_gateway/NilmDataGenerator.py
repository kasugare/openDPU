#!/usr/bin/env python
# -*- coding: utf-8 -*-

from nilm_worker.job_container.tajo_gateway.tajo_collector.TajoDataGenerator import TajoDataGenerator
import time

class NilmDataGenerator:
	def __init__(self, logger, sendMessageQ):
		self._logger = logger
		self._sendMessageQ = sendMessageQ

	def doProcess(self,	jobType, message):
		self._logger.info("##############################")
		self._logger.info("### DO Nilm Data Generator ###")
		self._logger.info("##############################")

		generator = TajoDataGenerator(self._logger, self._sendMessageQ)
		generator.doProcess(jobType, message)
