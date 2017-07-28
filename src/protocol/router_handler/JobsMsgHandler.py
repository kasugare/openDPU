#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_common import cvtWorkerId
from common.conf_system import getHeartBeatIntervalTime, getJobRetryCount
from protocol.message_pool.MessageGenerator import genResHB, genReqJobData
from dpu_master.health_manager.HealthChecker import HealthChecker
from ProtocolAnalyzer import ProtocolAnalyzer
from threading import Thread, Lock
import Queue
import time

class JobsMsgHandler(ProtocolAnalyzer):
	def __init__(self, logger, socketObj, resourceHandler, requestQ, resultQ):
		ProtocolAnalyzer.__init__(self, logger, socketObj)
		self._resourceHandler = resourceHandler
		self._workerObj = socketObj
		self._requestQ = requestQ
		self._resultQ = resultQ
		self._routerQ = Queue.Queue()
		self._workerId = cvtWorkerId(socketObj)
		self._resultMap = {}

		self._maxJobRetryCount = getJobRetryCount()
		self.doTerminsation = False
		self.isRunnable = True

	def initContext(self, _context):
		self._context = _context
		routeThread = Thread(target=self._runMessageRouter, args=(self._routerQ,))
		routeThread.setDaemon(1)
		routeThread.start()

	def routeProtocol(self, message):
		self._routerQ.put_nowait(message)

	def _runMessageRouter(self, routerQ):
		while True:
			try:
				message = routerQ.get()
				proto = self.analyzeMessage(message)
				self._logger.debug("# JMS : [%s] - message : %s"%(self._workerId, message))
				if proto == 'RES_HB':
					self.isRunnable = True

				elif proto == 'RES_OK':
					self.isRunnable = True

				elif proto == 'REQ_INIT_RESOURCE':
					self._logger.debug("# JMS : [%s] %s "%(self._workerId, proto))
					totalCpu = message['totalCpu']
					publicCpu = message['publicCpu']
					self._resourceHandler.addInitWorker(self._workerObj, totalCpu=totalCpu, publicCpu=publicCpu)
					self._genSubProcess(jobType = 'health_check')

				elif proto == 'REQ_JOB_SUCCES':
					jobType = message['jobType']
					jobId = message['jobId']
					taskId = message['taskId']
					result = message['result']
					self._logger.debug("# JMS : [%s] %s - jobType : %s"%(self._workerId, proto, jobType))
					if jobType == 'DPU_RAW_DATA' or jobType == 'DPU_STATUS_CHECK':
						self._logger.info("# [TAJO] [%s] data collection done" %(self._workerId))
						self._resourceHandler.delPrevWorkerObj(self._workerObj)
						self._resourceHandler.delCurrPriorityWorker(workerObj=self._workerObj)
					self._resourceHandler.setRunningToWaitCpu(self._workerObj, processType=message['processType'])
					self._resourceHandler.changeStatusToDoneJobDAG(jobId, taskId, result)
					self._resourceHandler.getTasksOnJobDAG(jobId)

					# if self._resourceHandler.checkJobTaskDone(jobId):
					# 	self._resultReducer(jobId)

				elif proto == 'REQ_JOB_FAIL':
					if message.has_key('error'):
						self._logger.error(message['error'])
					jobMessage = message['jobMsg']
					jobId = message['jobId']
					taskId = message['taskId']

					self._logger.warn("# JMS : [%s] Job failed, so it was roll back and going to retry running job by other worker." %(self._workerId))
					self._logger.debug("# JMS : [%s] %s - analysisType : %s"%(self._workerId, proto, jobMessage['params']['analysisType']))

					analysisType = None
					if jobMessage['params'].has_key('analysisType'):
						analysisType = jobMessage['params']['analysisType']

					if jobMessage['retry'] <= self._maxJobRetryCount:
						jobMessage['retry'] += 1
						jobProto = jobMessage['proto']

						if jobProto == 'REQ_DPU_STATUS_CHECK' or jobProto == 'REQ_GEN_DPU_RAW_DATA':
							self._resourceHandler.addPriorityJob((1,jobMessage))
						elif jobProto == 'REQ_DPU_LEARN_SCHEDULE':
							self._resourceHandler.addPrivateJob((1,jobMessage))
						elif jobProto == 'REQ_DPU_DATA':
							if analysisType == 'usage':
								self._resourceHandler.addPrivateJob((1,jobMessage))
							elif analysisType == 'meta':
								self._resourceHandler.addPrivateJob((2,jobMessage))
							elif analysisType == 'metaUpdate':
								self._resourceHandler.addPrivateJob((3,jobMessage))
						else:
							self._resourceHandler.addPrivateJob((4,jobMessage))
					else:
						self._logger.warn("# [%s] Exceed max retry count. job message : %s" %(self._workerId,  str(jobMessage)))
					self._resourceHandler.setRunningToWaitCpu(self._workerObj, processType=jobMessage['processType'])
					self._resourceHandler.changeStatusToFailJobDAG(jobId)


			except Exception, e:
				self._logger.exception(e)

	def _genSubProcess(self, jobType, message = None):
		self._logger.debug('- crete job process, job type : %s' %jobType)
		if jobType == 'health_check':
			jobThread = Thread(target=self._runHeartBeat, args=(getHeartBeatIntervalTime(), ))
		jobThread.setDaemon(1)
		jobThread.start()

	def _runHeartBeat(self, interval = 30):
		HealthChecker(self._logger, self).run(interval)

	def _resultReducer(self, jobId):
		taskSet = self._resourceHandler.getTasksOnJobDAG(jobId)
		resultSet = []
		for taskId in taskSet:
			resultSet.append(taskSet[taskSet]['result'])
		print resultSet




