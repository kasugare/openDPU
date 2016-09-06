#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from common.conf_network import getHostInfo
from dpu_master.client_listener.RequestHandler import RequestHandler
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread, Lock
import time

PROCESS_NAME = 'CLIENT_LISTENER'

class ClientListener:
	def __init__(self, clientQ):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('# process start : %s' %PROCESS_NAME)
		self._clientQ = clientQ
		self._reqQ = clientQ['reqQ']
		self._routerQ = clientQ['routerQ']

	def __del__(self):
		self._logger.warn('@ terminate process : %s' %PROCESS_NAME)

	def runServer(self):
		try:
			hostIp, hostPort = getHostInfo(PROCESS_NAME)
			svrsock = socket(AF_INET, SOCK_STREAM)
			svrsock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
			svrsock.bind((hostIp, hostPort))
			svrsock.listen(5)

			while True:
				socketObj, addr = svrsock.accept()
				clientListener = Thread(target=self._bindClientListener, args=(socketObj, addr))
				clientListener.setDaemon(1)
				clientListener.start()

				self._logger.info("# connected client")
				self._logger.info("- client addr : %s, port : %d" %(addr[0], addr[1]))
		except KeyboardInterrupt, ki:
			pass

	def _bindClientListener(self, socketObj, addr):
		pass

	def _socketClose(self, socketObj):
		try:
			if socketObj:
				socketObj.close()
				self._logger.info("# client connection closed")
		except Exception, e:
			self._logger.exception(e)