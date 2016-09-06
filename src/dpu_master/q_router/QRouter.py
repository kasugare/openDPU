#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from common.util_protocol_parser import parseProtocol
from threading import Thread, Lock
from QPack import QPack

class QRouter(QPack):
	def __init__(self, logger, dpuProcesses, clientQ, deployQ, jobQ, resourceQ, networkQ):
		QPack.__init__(self, logger, clientQ, deployQ, jobQ, resourceQ, networkQ)
		self._logger = logger
		self._dpuProcesses = dpuProcesses
		self._routerThreads = []

	def __del__(self):
		for process in self._dpuProcesses:
			process.join()
			if process.is_alive():
				process.terminate()
		self._closeQ()

	def doProcess(self):
		try:
			self._routerThreads.append(Thread(target=self._deployMsgListener, args=()))
			self._routerThreads.append(Thread(target=self._jobMsgListener, args=()))
			self._routerThreads.append(Thread(target=self._resourceMsgListener, args=()))
			self._routerThreads.append(Thread(target=self._networkMsgListener, args=()))
			for thread in self._routerThreads:
				thread.setDaemon(1)
				thread.start()
			self._clientMsgListener()
		except KeyboardInterrupt, e:
			self.__del__()
		except Exception, e:
			self._logger.exception(e)

	def _clientMsgListener(self):
		while True:
			reqMsg = self.clientRouterQ.get()
			print "[Q] QRouter(clientRouterQ) :", reqMsg
			protocol, statCode = parseProtocol(reqMsg)

	def _deployMsgListener(self):
		while True:
			reqMsg = self.deployRouterQ.get()
			print "[Q] QRouter(deployRouterQ) :", reqMsg
			protocol, statCode = parseProtocol(reqMsg)

	def _jobMsgListener(self):
		while True:
			reqMsg = self.jobRouterQ.get()
			print "[Q] QRouter(jobRouterQ) :", reqMsg
			protocol, statCode = parseProtocol(reqMsg)

	def _resourceMsgListener(self):
		while True:
			reqMsg = self.resourceRouterQ.get()
			print "[Q] QRouter(resourceRouterQ) :", reqMsg
			protocol, statCode = parseProtocol(reqMsg)

	def _networkMsgListener(self):
		while True:
			reqMsg = self.networkRouterQ.get()
			print "[Q] QRouter(networkRouterQ) :", reqMsg
			protocol, statCode = parseProtocol(reqMsg)
			if protocol == 'SYS_SET_RESOURCE':
				self.resourceReqQ.put_nowait(reqMsg)
			elif protocol == 'SYS_DEL_RESOURCE':
				self.resourceReqQ.put_nowait(reqMsg)

