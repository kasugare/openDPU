#!/usr/bin/env python
# -*- coding: utf-8 -*-

from threading import Thread, Lock
import pickle

class CollectorHandler:
	def __init__(self, logger, msgRouter, clientRequestJobQueue):
		self._logger = logger
		self._msgRouter = msgRouter
		self._clientRequestJobQueue = clientRequestJobQueue

	def doProcess(self):
		try:
			while True:
				recvMessage = pickle.loads(self._clientRequestJobQueue.get())
				self._logger.info('# Input job message : %s' %str(recvMessage))
				if not recvMessage:
					break
				self._msgRouter.routeProtocol(recvMessage)
		except Exception, e:
			self._logger.exception(e)