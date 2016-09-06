#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from common.util_protocol_parser import parseProtocol
from dpu_worker.q_router.QPack import QPack
from message_pool.system_protocol_messages import genGetWorkerResource, genGetSysWidMsg
from threading import Thread, Lock
import time

class QRouter(QPack):
	def __init__(self, logger, dpuProcesses, deployQ, jobQ, resourceQ, networkQ):
		QPack.__init__(self, logger, deployQ, jobQ, resourceQ, networkQ)
		self._logger = logger
		self._dpuProcesses = dpuProcesses
		self._routerThreads = []
		self._workerId = None

	def __del__(self):
		for process in self._dpuProcesses:
			process.join()
			if process.is_alive():
				process.terminate()
		self._closeQ()

	def doProcess(self, workerId):
		try:
			self._workerId = workerId
			self._routerThreads.append(Thread(target=self._deployMsgListener, args=()))
			self._routerThreads.append(Thread(target=self._resourceMsgListener, args=()))
			self._routerThreads.append(Thread(target=self._networkMsgListener, args=()))
			for thread in self._routerThreads:
				thread.setDaemon(1)
				thread.start()
			self._jobMsgListener()
		except KeyboardInterrupt, e:
			self.__del__()
		except Exception, e:
			self._logger.exception(e)

	def _deployMsgListener(self):
		while True:
			reqMsg = self.deployRouterQ.get()
			print "[Q] QRouter(deployRouterQ) :", reqMsg

	def _jobMsgListener(self):
		while True:
			reqMsg = self.jobRouterQ.get()
			print "[Q] QRouter(jobRouterQ) :", reqMsg

	def _resourceMsgListener(self):
		while True:
			reqMsg = self.resourceRouterQ.get()
			print "[Q] QRouter(resourceRouterQ) :", reqMsg
			protocol, statCode = parseProtocol(reqMsg)

			if protocol == 'SYS_RES_RESOURCE':
				self.networkRouterQ.put_nowait(reqMsg)

	def _networkMsgListener(self):
		while True:
			reqMsg = self.networkRouterQ.get()
			print "[Q] QRouter(networkReqQ) :", reqMsg
			protocol, statCode = parseProtocol(reqMsg)

			if protocol == 'SYS_GET_WID':
				tempId = reqMsg['tempId']
				self.networkReqQ.put_nowait(genGetSysWidMsg(tempId, self._workerId))
			if protocol == 'SYS_REQ_RESOURCE':
				self.resourceReqQ.put_nowait(reqMsg)
			elif protocol == 'SYS_RES_RESOURCE':
				self.networkReqQ.put_nowait(reqMsg)
