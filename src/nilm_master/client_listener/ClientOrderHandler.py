#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tcp_modules.NetworkHandler import DataReceiver, DataSender
from threading import Thread, Lock

class ClientOrderHandler:
	def __init__(self, logger, socketObj, msgRouter):
		self.receiver = DataReceiver(logger, socketObj)
		self.sender = DataSender(logger, socketObj)
		self._logger = logger
		self._msgRouter = msgRouter

	def doProcess(self):
		try:
			while True:
				recvMessage = self.receiver.recvMsg()
				if not recvMessage:
					break
				self._msgRouter.routeRequestJob(recvMessage)

		except Exception, e:
			self._logger.error("# Client socket closed!")
			self._logger.exception(e)


