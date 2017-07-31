#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_common import cvtWorkerId
from MasterResourcePool import MasterResourcePool
from threading import Lock
import Queue

class MasterResourceHandler(MasterResourcePool):
	def __init__(self, logger):
		MasterResourcePool.__init__(self, logger, DEFAULT_NAME = 'MASTER')
		logger.info("# Set master resources pool")
		self._lock = Lock()

	def addJob(self, jobMsg):
		self._lock.acquire()
		self._addJob(jobMsg)
		self._lock.release()

	def addPriorityJob(self, jobMsg):
		self._lock.acquire()
		self._addPriorityJob(jobMsg)
		self._lock.release()

	def addPrivateJob(self, jobMsg, processType='privateCpu'):
		self._lock.acquire()
		self._addPrivateJob(jobMsg)
		self._lock.release()

	def addPublicJob(self, jobMsg, processType='publicCpu'):
		self._lock.acquire()
		self._addPublicJob(jobMsg)
		self._lock.release()

	def addInitWorker(self, workerObj, totalCpu, publicCpu=0):
		self._lock.acquire()
		self._addInitWorker(workerObj, totalCpu, publicCpu)
		for _ in range(totalCpu):
			self._addWorker(workerObj)
		self._lock.release()

	def addWorker(self, workerObj):
		self._lock.acquire()
		self._addWorker(workerObj)
		self._lock.release()

	def assignPriorityWorker(self):
		self._logger.info("# [RESOURCE] assign priority worker")
		if self._priorityWorkerPool.qsize() >= 1 or not self._isTajoEnabled:
			return
		try:
			for _ in range(self._publicWorkerPool.qsize()):
				workerObj = self._publicWorkerPool.get_nowait()
				workerId = cvtWorkerId(workerObj)
				processType = 'publicCpu'
				if self._workerCpuStatus.has_key(workerId):
					idle = self._workerCpuStatus[workerId][processType]['idle']
					if idle > 0:
						self.setIdleToWaitCpu(workerObj, processType)
						self._masterWorkerPool.put_nowait(workerObj)
						return
					else:
						self._publicWorkerPool.put_nowait(workerObj)
		except Exception, e:
			self._logger.exception(e)

		try:
			for _ in range(self._privateWorkerPool.qsize()):
				workerObj = self._privateWorkerPool.get_nowait()
				workerId = cvtWorkerId(workerObj)
				processType = 'privateCpu'
				if self._workerCpuStatus.has_key(workerId):
					idle = self._workerCpuStatus[workerId][processType]['idle']
					if idle > 0:
						self.setIdleToWaitCpu(workerObj, processType)
						self._masterWorkerPool.put_nowait(workerObj)
						return
					else:
						self._privateWorkerPool.put_nowait(workerObj)
		except Exception, e:
			self._logger.exception(e)

	def resetWorkerQueue(self, closedWorkerId):
		self._logger.warn("# [RESOURCE] reset all workers : %s" %closedWorkerId)
		workerPools = {}
		workerPools['master'] = self._masterWorkerPool
		workerPools['priority'] = self._priorityWorkerPool
		workerPools['private'] = self._privateWorkerPool
		workerPools['public'] = self._publicWorkerPool

		if self._workerCpuStatus.has_key(closedWorkerId):
			del self._workerCpuStatus[closedWorkerId]

		for workerId in self._workerCpuStatus.keys():
			for processType in self._workerCpuStatus[workerId]:
				total = self._workerCpuStatus[workerId][processType]['total']
				running = self._workerCpuStatus[workerId][processType]['running']
				self._workerCpuStatus[workerId][processType]['idle'] = 0
				self._workerCpuStatus[workerId][processType]['wait'] = total - running

		for workerPoolType in workerPools.keys():
			workerPool = workerPools[workerPoolType]
			for i in range(workerPool.qsize()):
				try:
					workerObj = workerPool.get_nowait()
					if type(workerObj) == dict:
						workerPool.put_nowait(workerObj)
					else:
						workerId = cvtWorkerId(workerObj)
						if workerPoolType == 'priority':
							self.delPrevWorkerObj(workerObj)
						if workerId and not workerId == closedWorkerId:
							self._masterWorkerPool.put_nowait(workerObj)
				except Exception, e:
					self._logger.error(e)
					pass

		if self._prevWorkerObjs.has_key(closedWorkerId):
			del self._prevWorkerObjs[closedWorkerId]
			self.delCurrPriorityWorker(workerId=closedWorkerId)

	def setCurrPriorityWorker(self, workerObj):
		workerId = cvtWorkerId(workerObj)
		self._setCurrentPriorityWorker(workerId)


	def getCurrPriorityWorker(self):
		return self._getCurrentPriorityWorker()

	def delCurrPriorityWorker(self, workerObj=None, workerId=None):
		if workerObj:
			workerId = cvtWorkerId(workerObj)

		if workerId and workerId == self._getCurrentPriorityWorker():
			self._setCurrentPriorityWorker(workerId=None)
			self.setTajoEnableStatus(enabled=True)

	def setPrevWorkerObj(self, workerObj):
		self._lock.acquire()
		workerId = cvtWorkerId(workerObj)
		self._prevWorkerObjs[workerId] = workerObj
		self._lock.release()

	def delPrevWorkerObj(self, workerObj):
		self._lock.acquire()
		try:
			workerId = cvtWorkerId(workerObj)
			if self._prevWorkerObjs.has_key(workerId):
				del self._prevWorkerObjs[workerId]
			else:
				self._logger.warn("# prevWorkerObjs has not workerId : %s" %workerId)
		except Exception, e:
			self._logger.exception(e)

		if not self._prevWorkerObjs:
			self._isTajoEnabled = True
		self._lock.release()

	def setTajoEnableStatus(self, enabled):
		self._isTajoEnabled = enabled

	def setPriorityJobPool(self, priority, job):
		self._priorityJobPool.put_nowait((priority, job))

	def isTajoEnabled(self):
		return self._isTajoEnabled

	def getPrevWorkerObjs(self):
		return self._prevWorkerObjs

	def getSearchable(self):
		return self._tajoSearchable


	def getMasterWorkerPool(self):
		return self._masterWorkerPool

	def getPriorityWorkerPool(self):
		return self._priorityWorkerPool

	def getPrivateWorkerPool(self):
		return self._privateWorkerPool

	def getPublicWorkerPool(self):
		return self._publicWorkerPool


	def getPriorityJobPool(self):
		return self._priorityJobPool

	def getPrivateJobPool(self):
		return self._privateJobPool

	def getPublicJobPool(self):
		return self._publicJobPool

	def isSinkable(self, workerObj, processType):
		workerStatus = self._getWorkerCpuStatus(workerObj)
		if not workerStatus:
			workerId = cvtWorkerId(workerObj)
			self._logger.warn("# %s is not existed in workerCpuStatus table" %workerId)
			return False

		total = workerStatus[processType]['total']
		wait = workerStatus[processType]['wait']
		idle = workerStatus[processType]['idle']
		running = workerStatus[processType]['running']

		if total < wait or total < idle + running:
			self._logger.warn("# wrong cpu status, workerId : %s, processType : %s, total : %d, idle : %d, running : %d" %(workerId, processType, total, idle, running))
			return False

		if wait > 0:
			return True
		else:
			return False

	def setWaitToIdleCpu(self, workerObj, processType):
		self._lock.acquire()
		workerId = cvtWorkerId(workerObj)
		if not self._workerCpuStatus.has_key(workerId):
			self._logger.warn("wrong worker cpu status, workerId : %s" %workerId)

		self._workerCpuStatus[workerId][processType]['wait'] -= 1
		self._workerCpuStatus[workerId][processType]['idle'] += 1
		self._logger.debug("    - [%s] : W -> I : %s" %(workerId.ljust(15), str(self._workerCpuStatus[workerId])))
		self._lock.release()

	def setIdleToWaitCpu(self, workerObj, processType):
		self._lock.acquire()
		workerId = cvtWorkerId(workerObj)
		if not self._workerCpuStatus.has_key(workerId):
			self._logger.warn("wrong worker cpu status, workerId : %s" %workerId)

		self._workerCpuStatus[workerId][processType]['idle'] -= 1
		self._workerCpuStatus[workerId][processType]['wait'] += 1
		self._logger.debug(" - [%s] : I -> W : %s" %(workerId.ljust(15), str(self._workerCpuStatus[workerId])))
		self._lock.release()

	def setIdelToRunningCpu(self, workerObj, processType):
		self._lock.acquire()
		workerId = cvtWorkerId(workerObj)
		if not self._workerCpuStatus.has_key(workerId):
			self._logger.warn("wrong worker cpu status, workerId : %s" %workerId)

		self._workerCpuStatus[workerId][processType]['idle'] -= 1
		self._workerCpuStatus[workerId][processType]['running'] += 1
		self._logger.debug(" - [%s] : I -> R : %s" %(workerId.ljust(15), str(self._workerCpuStatus[workerId])))
		self._lock.release()

	def setRunningToWaitCpu(self, workerObj, processType):
		self._lock.acquire()
		workerId = cvtWorkerId(workerObj)
		if not self._workerCpuStatus.has_key(workerId):
			self._logger.warn("wrong worker cpu status, workerId : %s" %workerId)

		self._workerCpuStatus[workerId][processType]['wait'] += 1
		self._workerCpuStatus[workerId][processType]['running'] -= 1
		self._logger.debug(" - [%s] : R -> W : %s" %(workerId.ljust(15), str(self._workerCpuStatus[workerId])))
		self._addWorker(workerObj)
		self._lock.release()

	def getWorkerResoures(self):
		return self._workerCpuStatus

	def addClientObject(self, clientId, clientObject):
		self._addClientObject(clientId, clientObject)

	def getClientObject(self, clientId):
		if self._clientPool.has_key(clientId):
			return self._clientPool[clientId]
		return None

	def delClientObject(self, clientId):
		self._delClientObject(clientId)

	def clearClientPool(self):
		self._clearClientPool()


	# Job DAG
	def addIdleJobDAG(self, jobId, taskId, workerId=None):
		self._lock.acquire()
		if self._jobDAG.has_key(jobId):
			self._jobDAG[jobId][taskId] = {'status': 'idle', 'workerId': workerId, 'result': None}
		else:
			self._jobDAG[jobId] = {taskId: {'status': 'idle', 'workerId': workerId, 'result': None}}
		self._lock.release()

	def changeStatusToIdelJobDAG(self, jobId, taskId):
		self._lock.acquire()
		if self._jobDAG.has_key(jobId) and self._jobDAG[jobId].has_key(taskId):
			self._jobDAG[jobId][taskId]['status'] = 'idle'
		self._lock.release()

	def changeStatusToAssignJobDAG(self, jobId, taskId):
		self._lock.acquire()
		if self._jobDAG.has_key(jobId) and self._jobDAG[jobId].has_key(taskId):
			self._jobDAG[jobId][taskId]['status'] = 'assign'
		self._lock.release()

	def changeStatusToRunJobDAG(self, jobId, taskId, workerId):
		self._lock.acquire()
		if self._jobDAG.has_key(jobId) and self._jobDAG[jobId].has_key(taskId):
			self._jobDAG[jobId][taskId] = {'status': 'running', 'workerId': workerId, 'result': None}
		self._lock.release()

	def changeStatusToDoneJobDAG(self, jobId, taskId, result=None):
		self._lock.acquire()
		if self._jobDAG.has_key(jobId) and self._jobDAG[jobId].has_key(taskId):
			self._jobDAG[jobId][taskId]['status'] = 'done'
			self._jobDAG[jobId][taskId]['result'] = result
		self._lock.release()

	def changeStatusToFailJobDAG(self, jobId, taskId):
		self._lock.acquire()
		if self._jobDAG.has_key(jobId) and self._jobDAG[jobId].has_key(taskId):
			self._jobDAG[jobId][taskId]['status'] = 'fail'
		self._lock.release()

	def delJobIdOnDAG(self, jobId):
		self._lock.acquire()
		if self._jobDAG.has_key(jobId) and self._jobDAG[jobId].has_key(taskId):
			del self._jobDAG[jobId]
		self._lock.release()

	def getTasksOnJobDAG(self, jobId):
		self._logger.debug('# Job Status')
		# for taskId in self._jobDAG[jobId].keys():
		# 	self._logger.debug(' - Job ID: %s, Task ID: %d, Status: %s, Worker ID: %s, Result: %s' %(jobId, taskId, self._jobDAG[jobId][taskId]['status'], self._jobDAG[jobId][taskId]['workerId'], self._jobDAG[jobId][taskId]['result']))
		if self._jobDAG.has_key(jobId):
			return self._jobDAG[jobId]
		return None

	def checkJobTaskDone(self, jobId):
		self._logger.debug('# Check Job Done : %s' %jobId)
		jobCompleted = True
		if self._jobDAG.has_key(jobId):
			tasksMap = self._jobDAG[jobId]
			for taskId in tasksMap:
				if tasksMap[taskId]['status'] != 'done':
					jobCompleted = False
					return jobCompleted
			return jobCompleted
		return None