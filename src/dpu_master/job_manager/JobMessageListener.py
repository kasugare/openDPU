#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tcp_modules.NetworkHandler import DataReceiver, DataSender
import Queue

class JobMessageListener:
	def __init__(self, logger, socketObj, msgRouter, debugging=None):
		self._logger = logger
		self._socketObj = socketObj
		self._msgRouter = msgRouter
		self._receiver = DataReceiver(logger, socketObj, recvMessageQ=Queue.Queue(), debugging=debugging)
		self._msgRouter.initContext(self)

	def doProcess(self):
		try:
			messageQueue = self._receiver.getMessageQueue()
			while True:
				recvMessage = messageQueue.get()
				if type(recvMessage) == str and recvMessage == 'RESET':
					break
				if not recvMessage:
					break
				self._msgRouter.routeProtocol(recvMessage)

		except Exception, e:
			self._logger.exception(e)
