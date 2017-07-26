#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from protocol.message_pool.MessageGenerator import genResHB, genReqResourceStat, genReqJobCompelted, genReqJobFail, genStubNilmMlJobSuccess, genStubNilmMlJobFail
from nilm_worker.job_container.tajo_gateway.NilmDataGenerator import NilmDataGenerator
from nilm_worker.job_container.nilm.NilmRunner import NilmRunner
from nilm_worker.job_container.nilm_status_checker.NilmStatusChecker import NilmStatusChecker
from nilm_worker.job_container.nilm_learner_scheduler.LearnerScheduler import LearnerScheduler
from nilm_worker.job_container.nilm_machine_learning.NilmMachineLearning import NilmMachineLearning
from protocol.message_pool.MessageGenerator import genReqTajoEnable
from tcp_modules.NetworkHandler import DataSender
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
		self._rLogger = Logger("R_CORE").getLogger()
		self._statusLogger = Logger('NILM_STATUS').getLogger()
		self._learnerLogger = Logger('LEARNER_SCHEDULER').getLogger()
		self._mlLogger = Logger('NILM_MACHINE_LEARNING').getLogger()
		self._tajoGatewayLogger = Logger('TAJO_GATEWAY').getLogger()

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

			elif proto == 'REQ_GEN_NILM_RAW_DATA':
				self._logger.debug('- crete job process, job type : %s' %jobType)
				jobThread = Thread(target=self._runNilmDataGenerator, args=(processType, jobId, jobType, message))

			elif proto == 'REQ_NILM_DATA':
				self._logger.debug('- crete job process, job type : %s' %jobType)
				jobThread = Thread(target=self._runNilmJobProcess, args=(processType, jobId, jobType, message))

			elif proto == 'REQ_NILM_STATUS_CHECK':
				self._logger.debug('- crete job process, job type : %s' %jobType)
				jobThread = Thread(target=self._runNilmStatusChecker, args=(processType, jobId, jobType, message))

			elif proto == 'REQ_NILM_LEARN_SCHEDULE':
				self._logger.debug('- crete job process, job type : %s' %jobType)
				jobThread = Thread(target=self._runLearnScheduler, args=(processType, jobId, jobType))

			elif proto == 'REQ_STUB_NILM_ML':
				self._logger.debug('- crete job process, job type : %s' %jobType)
				stubId = message['stubId']
				params = message['params']
				processInfo = message['processInfo']
				mlProcess = Process(target=self._runNilmMarchinLearingForPredict, args=(stubId, params, processInfo))
				self._MlSubprocessTable[mlProcess.pid] = mlProcess
				mlProcess.start()

			if jobThread:
				jobThread.setDaemon(1)
				jobThread.start()


	def _runNilmDataGenerator(self, processType, jobId, jobType, message):
		self._resourceManager.assignCpuByProcessType(processType)
		try:
			dataGenerator = NilmDataGenerator(self._tajoGatewayLogger, self._sendMessageQ)
			dataGenerator.doProcess(jobType, message)

			self._resourceManager.returnCpuByProcessType(processType)
			sendMessage = genReqJobCompelted(jobId, availCpu=self._resourceManager.getAvailCpu(), jobType=jobType, processType=processType)
			self._sendMessageQ.put_nowait(sendMessage)
		except Exception, e:
			self._resourceManager.returnCpuByProcessType(processType)
			sendMessage = genReqJobFail(jobId, availCpu=self._resourceManager.getAvailCpu(), message=message, error=str(e), processType=processType)
			self._sendMessageQ.put_nowait(sendMessage)
			self._logger.error("# Job task be failed.")
			self._logger.exception(e)


	def _runNilmJobProcess(self, processType, jobId, jobType, message):
		paramElements = ['analysisType', 'startTS', 'endTS', 'sid', 'did', 'lfid', 'filePath']
		if not message.has_key('params'):
			self._logger.warn("# This job task is wrong params. this job failed.")
			return
		params = message['params']
		for element in paramElements:
			if not params.has_key(element):
				self._logger.warn("# This job task is wrong params. this job failed.")
				return

		self._resourceManager.assignCpuByProcessType(processType)
		try:
			self._logger.info('# [%s] Do process nilm jobs,  analysis type : %s' %(jobId, params['analysisType']))

			nilmRunner = NilmRunner(self._logger, self._rLogger, jobId, self._outputQ, self._sendMessageQ)
			nilmRunner.doProcess(params)

			self._resourceManager.returnCpuByProcessType(processType)
			sendMessage = genReqJobCompelted(jobId, availCpu=self._resourceManager.getAvailCpu(), jobType="NILM_RUN", processType=processType)
			self._sendMessageQ.put_nowait(sendMessage)
		except Exception, e:
			self._resourceManager.returnCpuByProcessType(processType)
			sendMessage = genReqJobFail(jobId, availCpu=self._resourceManager.getAvailCpu(), message=message, error=str(e), processType=processType)
			self._sendMessageQ.put_nowait(sendMessage)
			self._logger.error("# Job task be failed.")
			self._logger.exception(e)
			

	def _runNilmStatusChecker(self, processType, jobId, jobType, message):
		self._resourceManager.assignCpuByProcessType(processType)
		try:
			statusChecker = NilmStatusChecker(logger=self._statusLogger)
			statusChecker.doProcess(jobType, message)

			self._resourceManager.returnCpuByProcessType(processType)
			sendMessage = genReqTajoEnable(jobType)
			self._sendMessageQ.put_nowait(sendMessage)
			sendMessage = genReqJobCompelted(jobId, availCpu=self._resourceManager.getAvailCpu(), jobType=jobType, processType=processType)
			self._sendMessageQ.put_nowait(sendMessage)
		except Exception, e:
			self._resourceManager.returnCpuByProcessType(processType)
			sendMessage = genReqJobFail(jobId, availCpu=self._resourceManager.getAvailCpu(), message=message, error=str(e), processType=processType)
			self._sendMessageQ.put_nowait(sendMessage)
			self._logger.error("# Job task be failed.")
			self._logger.exception(e)


	def _runLearnScheduler(self, processType, jobId, jobType):
		self._resourceManager.assignCpuByProcessType(processType)
		try:
			loadBalancer = LearnerScheduler(logger=self._learnerLogger)
			loadBalancer.doProcess()

			self._resourceManager.returnCpuByProcessType(processType)
			sendMessage = genReqJobCompelted(jobId, availCpu=self._resourceManager.getAvailCpu(), jobType=jobType, processType=processType)
			self._sendMessageQ.put_nowait(sendMessage)
		except Exception, e:
			self._resourceManager.returnCpuByProcessType(processType)
			sendMessage = genReqJobFail(jobId, availCpu=self._resourceManager.getAvailCpu(), message=message, error=str(e), processType=processType)
			self._sendMessageQ.put_nowait(sendMessage)
			self._logger.error("# Job task be failed.")
			self._logger.exception(e)

	def _runNilmMarchinLearingForPredict(self, stubId, params, processInfo):
		try:
			machineLearning = NilmMachineLearning(logger=self._mlLogger)
			nilmOutput = machineLearning.doProcess(processInfo, params)
			
			resultMessage = genStubNilmMlJobSuccess(stubId, nilmOutput)
			resultMessage['processInfo'] = processInfo
			self._stubResultQ.put_nowait(resultMessage)
		except Exception, e:
			self._logger.error("# Job task be failed.")
			self._logger.exception(e)
			result = {
				"processInfo": processInfo,
				"result": params,
				"error": e
			}
			resultMessage = genStubNilmMlJobFail(stubId, result)
			resultMessage['processInfo'] = processInfo
			self._stubResultQ.put_nowait("Fail")
		# os.kill(os.getpid(), signal.SIGTERM)
