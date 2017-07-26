#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from common.conf_network import getHostInfo
from resource_handler.WorkerResourceHandler import WorkerResourceHandler
from job_container.nilm.OutputManager import OutputManager
from worker_stub.WorkerStub import WorkerStub
from protocol.MessageRouter import MessageRouter
from WorkerHandler import WorkerHandler
from socket import socket, AF_INET, SOCK_STREAM
from threading import Thread, Lock
import multiprocessing
import signal
import random
import Queue
import time
import sys

PROCESS_NAME = 'JOB_WORKER'

def signal_handler(signal, frame):
	print "NilmClusterWorker"
	print 'You pressed Ctrl+C!'
	sys.exit(0)

class NilmClusterWorker:
	def __init__(self):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._resourceHandler = WorkerResourceHandler(self._logger)
		self.msgRouterObj = MessageRouter(self._logger, self._resourceHandler)
		self._outputQ = multiprocessing.Queue()
		self._jobRequestQ = multiprocessing.Queue()
		self._stubResultQ = multiprocessing.Queue()
		self.retryIntervalTime = 10

		self._logger.info('---------- [ Job Worker Manager ] ----------')
		self._logger.info("# Start job worker")
		signal.signal(signal.SIGINT, signal_handler)

	def doProcess(self):
		time.sleep(float(random.randrange(5,15))/4)
		try:
			self._initProcess()
		except Exception,e:
			self._logger.exception(e)

		while True:
			try:
				# time.sleep(float(random.randrange(5,15))/10)
				socketObj, workerProcess = self._connServer()
				self._logger.debug('- closed socket object : %s' %str(socketObj))
				self._logger.debug('- socketObj : %s' %str(workerProcess))
			except Exception, e:
				self._logger.error("-----------------------------------")
				self._logger.error("# Server not response or not ready.")
				self._logger.error("-----------------------------------")
				self._logger.exception(e)
			finally:
				if self.retryIntervalTime < 600:
					time.sleep(self.retryIntervalTime)
				else:
					time.sleep(self.retryIntervalTime)
					self.retryIntervalTime += 1

	def _connServer(self):
		hostIp, hostPort = getHostInfo('JOB_MANAGER')
		socketObj = socket(AF_INET, SOCK_STREAM)
		socketObj.connect((hostIp, hostPort))
		msgRouter = self.msgRouterObj.getJobWorkerRouter(socketObj, self._outputQ, self._jobRequestQ, self._stubResultQ)
		worker = WorkerHandler(self._logger, socketObj, msgRouter)
		worker.doProcess()
		return socketObj, worker

	def _initProcess(self):
		jobProcess = Thread(target=self._initOutputManager, args=(self._outputQ,))
		jobProcess.setDaemon(1)
		jobProcess.start()

		jobProcess = Thread(target=self._initJobStub, args=())
		jobProcess.setDaemon(1)
		jobProcess.start()

	def _initOutputManager(self, outputQ):
		try:
			outputManager = OutputManager(outputQ)
			outputManager.run()
		except Exception, e:
			self._logger.exception(e)

	def _initJobStub(self):
		stub = WorkerStub(self._resourceHandler, self._jobRequestQ, self._stubResultQ)
		stub.runProcess()
