#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_network import getWorkerStubInfo
from common.util_logger import Logger
from common.util_common import cvtWorkerId
from tcp_modules.NetworkHandler import DataReceiver
from StubJobRouter import StubJobRouter
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread, Lock
import multiprocessing


PROCESS_NAME = 'WORKER_STUB'

class WorkerStub:
	def __init__(self, resourceHandler, jobRequestQ, stubResultQ):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._resourceHandler = resourceHandler
		self._jobRequestQ = jobRequestQ
		self._stubReqeustQ = multiprocessing.Queue()
		self._stubResultQ = stubResultQ
		self._stubMappingTable = {}
		self._initStubJobRouter()

	def _initStubJobRouter(self):
		stubJobRoutingThread = Thread(target=self._runStubJobRouter, args=())
		stubJobRoutingThread.setDaemon(1)
		stubJobRoutingThread.start()

	def _runStubJobRouter(self):
		stubJobRouter = StubJobRouter(self._logger, self._resourceHandler, self._jobRequestQ, self._stubReqeustQ, self._stubResultQ, self._stubMappingTable)
		stubJobRouter.runJobRequest()

	def runProcess(self):
		hostIp, hostPort = getWorkerStubInfo(PROCESS_NAME)
		svrsock = socket(AF_INET, SOCK_STREAM)
		svrsock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		svrsock.bind((hostIp, hostPort))
		svrsock.listen(5)

		while True:
			try:
				socketObj, addr = svrsock.accept()
				requestHandler = Thread(target=self._bindStubRequest, args=(socketObj,))
				requestHandler.setDaemon(1)
				requestHandler.start()
			except Exception, e:
				self._logger.exception(e)

	def _bindStubRequest(self, socketObj):
		stubId = cvtWorkerId(socketObj)
		self._stubMappingTable[stubId] = socketObj

		dataReceiver = DataReceiver(self._logger, socketObj)
		dataReceiver.recvMsgThroughQ(self._stubReqeustQ)
