#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_common import cvtWorkerId
from threading import Lock
import Queue

class ClientJobResultListener:
	def __init__(self, logger, resourceHandler, jobResultQ):
		logger.info("# Start result-job listener")
		self._logger = logger
		self._resourceHandler = resourceHandler
		self._jobResultQ = jobResultQ

	def routeResultJobProtocol(self):
		while True:
			try:
				resultMessage = self._jobResultQ.get()
				clientId = resultMessage['clientId']
				clientObject = self._resourceHandler.getClientObject(clientId)
				if clientObj:
					clientObject.routeResponsJob(resultMessage)
			except Exception, e:
				self._logger.exception(e)
