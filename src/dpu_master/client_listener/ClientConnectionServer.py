#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_network import getHostInfo
from common.util_common import cvtWorkerId
from protocol.MessageRouter import MessageRouter
from ClientOrderHandler import ClientOrderHandler
from protocol.router_handler.ClientJobResultListener import ClientJobResultListener
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread, Lock

PROCESS_NAME = 'MASTER'

class ClientConnectionServer:
	def __init__(self, logger, resourceHandler, jobQ, jobResultQ):
		self._logger = logger
		self._jobQ = jobQ
		self._jobResultQ = jobResultQ
		self._resourceHandler =resourceHandler
		self.msgRouterObj = MessageRouter(logger, resourceHandler)
		self._initResultListener()

	def _initResultListener(self):
		resultListener = Thread(target=self._runClientJobResultListener, args=())
		resultListener.setDaemon(1)
		resultListener.start()

	def runServer(self):
		self._logger.info("# Start client connection server")
		hostIp, hostPort = getHostInfo(PROCESS_NAME)
		svrsock = socket(AF_INET, SOCK_STREAM)
		svrsock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		svrsock.bind((hostIp, hostPort))
		svrsock.listen(5)

		while True:
			try:
				socketObj, addr = svrsock.accept()
				requestHandler = Thread(target=self._bindClientRequest, args=(socketObj, addr))
				requestHandler.setDaemon(1)
				requestHandler.start()
				clientId = cvtWorkerId(socketObj)
				self._logger.info("# connected client : %s" %clientId)
			except Exception, e:
				self._logger.exception(e)

	def _bindClientRequest(self, socketObj, addr):
		msgRouter = self.msgRouterObj.getMasterMsgRouter(socketObj, self._jobQ)
		ClientOrderHandler(self._logger, socketObj, msgRouter).doProcess()
		self._socketClose(socketObj)

	def _runClientJobResultListener(self):
		jobResultListener = ClientJobResultListener(self._logger, self._resourceHandler, self._jobResultQ)
		jobResultListener.routeResultJobProtocol()


	def _socketClose(self, socketObj):
		try:
			if socketObj:
				clientId = cvtWorkerId(socketObj)
				socketObj.close()
				self._logger.info("# client connection closed : %s" %clientId)
		except Exception, e:
			self._logger.exception(e)
