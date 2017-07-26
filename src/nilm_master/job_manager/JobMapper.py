#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_common import cvtWorkerId
from tcp_modules.NetworkHandler import DataSender
from threading import Thread, Lock
import Queue
import time

class JobMapper:
	def __init__(self, logger, requestQ, resourceHandler):
		self._logger = logger
		self._lock = Lock()
		self._runnable = True
		self._resourceHandler = resourceHandler

		self._masterWorkerPool = resourceHandler.getMasterWorkerPool()
		self._priorityWorkerPool = resourceHandler.getPriorityWorkerPool()
		self._privateWorkerPool = resourceHandler.getPrivateWorkerPool()
		self._publicWorkerPool = resourceHandler.getPublicWorkerPool()

		self._priorityJobPool = resourceHandler.getPriorityJobPool()
		self._privateJobPool = resourceHandler.getPrivateJobPool()
		self._publicJobPool = resourceHandler.getPublicJobPool()

		self._initMapper()

	def __del__(self):
		self._runnable = False

	def _initMapper(self):
		self._mapperList = []
		self._mapperList.append(Thread(target=self._runWorkerRebalancer, args=()))
		self._mapperList.append(Thread(target=self._runPriorityJobMapper, args=()))
		self._mapperList.append(Thread(target=self._runPrivateJobMapper, args=()))
		# self._mapperList.append(Thread(target=self._runPublicJobMapper, args=()))
		
		for mapper in self._mapperList:
			mapper.setDaemon(1)
			mapper.start()

	def _restartMapper(self):
		self._runnable = True
		for mapper in self._mapperList:
			if not mapper.isAlive():
				mapper.setDaemon(1)
				mapper.start()

	def runProcess(self):
		while True:
			try:
				workerObj = self._masterWorkerPool.get()
				if type(workerObj) == dict:
					if workerObj.has_key('RESET'):
						closedWorkerId = workerObj['RESET']
						self._runnable = False
						self._resourceHandler.resetWorkerQueue(closedWorkerId)
						self._restartMapper()
					self._printResourceStatus()
				else:
					prevWorkerObjs = self._resourceHandler.getPrevWorkerObjs()
					workerId = cvtWorkerId(workerObj)
					if self._priorityWorkerPool.qsize() < 1 and self._resourceHandler.isTajoEnabled() and not prevWorkerObjs.has_key(workerId):
						if self._resourceHandler.isSinkable(workerObj, processType='publicCpu'):
							self._resourceHandler.setWaitToIdleCpu(workerObj, processType='publicCpu')
							self._resourceHandler.setPrevWorkerObj(workerObj)
							self._priorityWorkerPool.put_nowait(workerObj)
						elif self._resourceHandler.isSinkable(workerObj, processType='privateCpu'):
							self._resourceHandler.setWaitToIdleCpu(workerObj, processType='privateCpu')
							self._resourceHandler.setPrevWorkerObj(workerObj)
							self._priorityWorkerPool.put_nowait(workerObj)
						else:
							self._logger.warn("# It has not assigning processes for priority workers")
							self._masterWorkerPool.put_nowait(workerObj)
							time.sleep(1)
					else:
						if self._resourceHandler.isSinkable(workerObj, processType='privateCpu'):
							self._resourceHandler.setWaitToIdleCpu(workerObj, processType='privateCpu')
							self._privateWorkerPool.put_nowait(workerObj)
						elif self._resourceHandler.isSinkable(workerObj, processType='publicCpu'):
							self._resourceHandler.setWaitToIdleCpu(workerObj, processType='publicCpu')
							self._publicWorkerPool.put_nowait(workerObj)
						else:
							self._logger.warn("# It has not assigning processes for common workers")
							self._masterWorkerPool.put_nowait(workerObj)
							time.sleep(1)
					self._printResourceStatus()
			except Exception, e:
				self._logger.exception(e)
				time.sleep(1)

	def _runWorkerRebalancer(self):
		while True:
			try:
				if self._priorityWorkerPool.qsize() == 0 and self._resourceHandler.isTajoEnabled() and self._priorityJobPool.qsize() > 0 and self._publicWorkerPool.qsize() > 0:
					self._logger.debug("# worker rebalance")
					resetWorkerObj = {'RESET':'master'}
					self._masterWorkerPool.put_nowait(resetWorkerObj)
			except Exception, e:
				self._logger.exception(e)
			time.sleep(60)

	def _runPriorityJobMapper(self):
		processType = 'publicCpu'
		while self._runnable:
			try:
				requestJob = self._priorityJobPool.get()
				jobMessage = requestJob[1]
				jobMessage['processType'] = processType
				workerObj = self._priorityWorkerPool.get()
				self._resourceHandler.setTajoEnableStatus(False)
				self._resourceHandler.setCurrPriorityWorker(workerObj)

				time.sleep(0.1)
				if not DataSender(self._logger, workerObj).sendMsg(jobMessage):
					self._priorityJobPool.put_nowait(requestJob)
					self._resourceHandler.setTajoEnableStatus(True)
					self._resourceHandler.delPrevWorkerObj(workerObj)
					self._resourceHandler.delCurrPriorityWorker(workerObj=workerObj)
				else:
					self._resourceHandler.setIdelToRunningCpu(workerObj, processType)
			except Exception, e:
				self._logger.exception(e)
			self._printResourceStatus()

	def _runPrivateJobMapper(self):
		while self._runnable:
			try:
				self._printResourceStatus()
				requestJob = self._privateJobPool.get()
				jobMessage = requestJob[1]
				analysisType = None

				if jobMessage.has_key('params') and jobMessage['params'].has_key('analysisType'):
					analysisType = jobMessage['params']['analysisType']

				if analysisType == 'meta' or analysisType == 'metaUpdate':
					processType = 'publicCpu'
					workerObj = self._publicWorkerPool.get()
				else:
					if self._privateWorkerPool.qsize() > 0:
						processType = 'privateCpu'
						workerObj = self._privateWorkerPool.get()
					else:
						processType = 'publicCpu'
						workerObj = self._publicWorkerPool.get()
					
				jobMessage['processType'] = processType
				time.sleep(0.2)
				if not DataSender(self._logger, workerObj).sendMsg(jobMessage):
					self._privateJobPool.put_nowait(requestJob)
				else:
					self._resourceHandler.setIdelToRunningCpu(workerObj, processType)
			except Exception, e:
				self._logger.exception(e)
			self._printResourceStatus()

	def _runPublicJobMapper(self):
		processType = 'publicCpu'
		while self._runnable:
			try:
				requestJob = self._publicJobPool.get()
				jobMessage = requestJob[1]
				jobMessage['processType'] = processType
				workerObj = self._publicWorkerPool.get()
				if not DataSender(self._logger, workerObj).sendMsg(jobMessage):
					self._publicWorkerPool.put_nowait(requestJob)
				else:
					self._resourceHandler.setIdelToRunningCpu(workerObj, processType)
			except Exception, e:
				self._logger.exception(e)
			self._printResourceStatus()

	def _printResourceStatus(self):
		self._lock.acquire()
		tajoEnabled = str(self._resourceHandler.isTajoEnabled())
		workerSize = len(self._resourceHandler.getPrevWorkerObjs())
		self._logger.info("[resource]  pi_WQ : %d,   pu_WQ : %d,   pv_WQ: %d,   pi_JQ : %d,   pv_JQ : %d,   priority enabled : %s,   priority worker size : %d" %(self._priorityWorkerPool.qsize(), self._privateWorkerPool.qsize(), self._publicWorkerPool.qsize(), self._priorityJobPool.qsize(), self._privateJobPool.qsize(), tajoEnabled, workerSize))
		self._lock.release()
		
