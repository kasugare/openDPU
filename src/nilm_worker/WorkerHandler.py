#!/usr/bin/env python
# -*- coding: utf-8 -*-

from socket import socket, AF_INET, SOCK_STREAM
from common.conf_network import getHostInfo
from common.util_logger import Logger
from tcp_modules.NetworkHandler import DataReceiver, DataSender
import signal
import time
import sys

PROCESS_NAME = 'JOB_WORKER'
def signal_handler(signal, frame):
	print "WorkerHandler"
	print 'You pressed Ctrl+C!'
	sys.exit(0)

class WorkerHandler:
	def __init__(self, logger, socketObj, msgRouter):
		self._logger = logger
		self._socketObj = socketObj
		self._msgRouter = msgRouter
		self._receiver = DataReceiver(logger, socketObj)
		self._sender = DataSender(logger, socketObj)
		self._maxRetry = 5
		signal.signal(signal.SIGINT, signal_handler)

	def __del__(self):
		signal.signal(signal.SIGINT, signal_handler)

	def doProcess(self):
		self._msgRouter.initPorcess()
		reconnCount = 0
		
		while reconnCount < self._maxRetry:
			try:
				recvMessage = self._receiver.recvMsg()
				if recvMessage:
					self._msgRouter.routeProtocol(recvMessage)
					reconnCount = 0
				else:
					self._logger.error("# server connection lost")
					reconnCount += 1
					time.sleep(1)
			except Exception, e:
				self._logger.error('# server socket closed!')
				self._logger.exception(e)
				reconnCount += 1
				time.sleep(1)
		try:
			self._socketObj.close()
		except Exception, e:
			self._logger.exception(e)
