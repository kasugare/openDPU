#!/usr/bin/env python
# -*- coding: utf-8 -*-

from router_handler.ClientMessageHandler import ClientMessageHandler
from router_handler.CollectorsMsgHandler import CollectorsMsgHandler
from router_handler.JobsMsgHandler import JobsMsgHandler
from router_handler.JobWorkersHandler import JobWorkersHandler

class MessageRouter:
	def __init__(self, logger, resourceManager):
		self._logger = logger
		self._resourceManager = resourceManager

	def getMasterMsgRouter(self, socketObj, jobQ):
		return ClientMessageHandler(self._logger, socketObj, self._resourceManager, jobQ)

	def getCollectorMsgRouter(self):
		return CollectorsMsgHandler(self._logger, self._resourceManager)

	def getJobMsgRouter(self, socketObj, requestQ, jobResultQ):
		return JobsMsgHandler(self._logger, socketObj, self._resourceManager, requestQ, jobResultQ)

	def getJobWorkerRouter(self, socketObj, outputQ, jobRequestQ, stubResultQ):
		return JobWorkersHandler(self._logger, socketObj, self._resourceManager, outputQ, jobRequestQ, stubResultQ)
