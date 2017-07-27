#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_network import getHostInfo
from common.util_common import cvtWorkerId
from common.util_logger import Logger
from dpu_master.job_manager.JobMessageListener import JobMessageListener
from dpu_master.job_manager.JobMapper import JobMapper
from protocol.MessageRouter import MessageRouter
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread, Lock
import multiprocessing

PROCESS_NAME = 'JOB_MANAGER'

class JobManager:
	def __init__(self, resourceHandler, jobResultQ):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('---------- [ Job Cluster Manager ] ----------')
		self._logger.info("# Start job management server")
		self.msgRouterObj = MessageRouter(self._logger, resourceHandler)
		self._resourceHandler = resourceHandler
		self._requestQ = multiprocessing.Queue()
		self._jobResultQ = jobResultQ
		self._debugging = Logger("DEBUGGING").getLogger()

	def __del__(self):
		self._logger.info('# Job Cluster Manager Terminate...')
		self._logger.info('---------------------------------------------')

	def runServer(self):
		try:
			mapperThread = Thread(target=self._runJobMapper, args=(self._resourceHandler, ))
			mapperThread.setDaemon(1)
			mapperThread.start()

			hostIp, hostPort = getHostInfo(PROCESS_NAME)
			svrsock = socket(AF_INET, SOCK_STREAM)
			svrsock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
			svrsock.bind((hostIp, hostPort))
			svrsock.listen(5)

			while True:
				socketObj, addr = svrsock.accept()
				requestHandler = Thread(target=self._bindClientRequest, args=(socketObj, addr))
				requestHandler.setDaemon(1)
				requestHandler.start()

				self._logger.info("# Connected Job Worker, worker addr : %s, port : %d" %(addr[0], addr[1]))
		except Exception, e:
			self._logger.exception(e)

	def _bindClientRequest(self, socketObj, addr):
		workerId = cvtWorkerId(socketObj)
		self._msgRouter = self.msgRouterObj.getJobMsgRouter(socketObj, self._requestQ, self._jobResultQ)
		JobMessageListener(self._logger, socketObj, self._msgRouter, debugging=self._debugging).doProcess()
		self._socketClose(socketObj, workerId)

	def _socketClose(self, socketObj, workerId=None):
		try:
			if socketObj:
				masterWorkerPool = self._resourceHandler.getMasterWorkerPool()
				try:
					socketId = '%s_%d' %(socketObj.getpeername()[0], socketObj.getpeername()[1])
					masterWorkerPool.put({"RESET":socketId})
				except Exception, e:
					masterWorkerPool.put({"RESET":workerId})

				if socketObj in self._resourceHandler.getPrevWorkerObjs():
					self._resourceHandler.delPrevWorkerObj(socketObj)
					self._resourceHandler.setTajoEnableStatus(True)
				socketObj.close()
				self._logger.info("# client connection closed")
		except Exception, e:
			self._logger.exception(e)

	def _runJobMapper(self, _resourceHandler):
		mapper = JobMapper(self._logger, self._requestQ, _resourceHandler)
		mapper.runProcess()
