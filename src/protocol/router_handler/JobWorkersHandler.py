#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from protocol.message_pool.MessageGenerator import genResHB, genReqResourceStat, genReqJobCompelted, genReqJobFail
from protocol.message_pool.MessageGenerator import genReqTajoEnable
from tcp_modules.NetworkHandler import DataSender
from dpu_worker.job_container.data_etl.DataEtlManager import DataEtlManager
from ProtocolAnalyzer import ProtocolAnalyzer
from multiprocessing import Process
from threading import Thread, Lock
import multiprocessing
import signal
import time
import sys
import os

class JobWorkersHandler(ProtocolAnalyzer):
	def __init__(self, logger, socketObj, resourceManager, outputQ, jobRequestQ, stubResultQ):
		ProtocolAnalyzer.__init__(self, logger, socketObj)
		self.__initLogger__()
		self._logger = logger
		self._resourceManager = resourceManager
		self._sendMessageQ = multiprocessing.Queue()
		self._outputQ = outputQ
		self._jobRequestQ = jobRequestQ
		self._stubResultQ = stubResultQ
		self._sender = DataSender(logger, socketObj)
		self._MlSubprocessTable = {}
		self._tajoCount = 0

	def __del__(self):
		for pid in self._MlSubprocessTable.keys():
			os.kill(pid, signal.SIGTERM)

	def __initLogger__(self):
		self._statusLogger = Logger('DPU_STATUS').getLogger()
		self._dataEtlLogger= Logger('DATA_ETL').getLogger()

	def initPorcess(self):
		totalCpu = self._resourceManager.getTotalCpu()
		publicCpu = self._resourceManager.getPublicCpu()

		sendThread = Thread(target=self._runDataSender, args=(self._sender, self._sendMessageQ))
		sendThread.setDaemon(1)
		sendThread.start()

		routeThread = Thread(target=self._runMessageRouter, args=(self._jobRequestQ,))
		routeThread.setDaemon(1)
		routeThread.start()

		sendMessage = genReqResourceStat(totalCpu, publicCpu)
		self._sendMessageQ.put_nowait(sendMessage)

	def _runDataSender(self, sender, sendMessageQ):
		while True:
			sendMessage = sendMessageQ.get()
			sender.sendMsg(sendMessage)
			time.sleep(0.1)

	def routeProtocol(self, message):
		self._jobRequestQ.put_nowait(message)

	def _runMessageRouter(self, routerQ):
		while True:
			message = routerQ.get()
			if type(message) != dict or not message.has_key('proto'):
				continue
			proto = message['proto']
			jobThread = None

			if not proto:
				continue

			if message.has_key('jobType'):
				jobType = (message['jobType']).upper()
				jobId = '%s_%d' %(jobType, time.time())
			else:
				jobType = None
				jobId = None

			if message.has_key('processType'):
				processType = message['processType']

			if proto == 'REQ_HB':
				self._logger.debug('- available cpu : %d' %self._resourceManager.getAvailCpu())
				self._sendMessageQ.put_nowait(genResHB())

			elif proto == 'REQ_GEN_DPU_RAW_DATA':
				self._logger.debug('- crete job process, job type : %s' %jobType)
				jobThread = Thread(target=self._runDpuDataGenerator, args=(processType, jobId, jobType, message))

			if jobThread:
				jobThread.setDaemon(1)
				jobThread.start()


	def _runDpuDataGenerator(self, processType, jobId, jobType, message):
		self._resourceManager.assignCpuByProcessType(processType)
		try:
			dataEtlManager = DataEtlManager(self._dataEtlLogger)
			dataEtlManager.doProcess()

			self._resourceManager.returnCpuByProcessType(processType)
			sendMessage = genReqJobCompelted(jobId, availCpu=self._resourceManager.getAvailCpu(), jobType=jobType, processType=processType)
			self._sendMessageQ.put_nowait(sendMessage)
		except Exception, e:
			self._resourceManager.returnCpuByProcessType(processType)
			sendMessage = genReqJobFail(jobId, availCpu=self._resourceManager.getAvailCpu(), message=message, error=str(e), processType=processType)
			self._sendMessageQ.put_nowait(sendMessage)
			self._logger.error("# Job task be failed.")
			self._logger.exception(e)
