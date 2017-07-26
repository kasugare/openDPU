#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_common import cvtWorkerId
from threading import Lock
import Queue
import os

class MasterResourcePool:
	def __init__(self, logger, DEFAULT_NAME):
		logger.info("# Created Resource Pool")
		self._logger = logger

		self._masterWorkerPool = Queue.Queue()
		self._priorityWorkerPool = Queue.Queue()
		self._privateWorkerPool = Queue.Queue()
		self._publicWorkerPool = Queue.Queue()

		self._priorityJobPool = Queue.PriorityQueue()
		self._privateJobPool = Queue.PriorityQueue()
		self._publicJobPool = Queue.PriorityQueue()
		self._workerCpuStatus = {}

		self._clientPool = {}

		self._currPriorityWorker = None
		self._prevWorkerObjs = {}
		
		self._tajoSearchable = True
		self._isTajoEnabled = True
		

	def _resetPrivateCpu(self, resetCore):
		self._privateCpu = resetCore
		return self._privateCpu


	def _addPriorityJob(self, jobMsg):
		self._priorityJobPool.put(jobMsg)

	def _addPrivateJob(self, jobMsg):
		self._privateJobPool.put(jobMsg)

	def _addPublicJob(self, jobMsg):
		self._publicJobPool.put(jobMsg)

	def _addJob(self, jobMsg):
		self._privateJobPool.put(jobMsg)


	def _addWorker(self, workerObj):
		self._masterWorkerPool.put(workerObj)


	def _addInitWorker(self, workerObj, totalCpu=0, publicCpu=0):
		workerId = cvtWorkerId(workerObj)
		privateCpu = totalCpu - publicCpu
		self._workerCpuStatus[workerId] = {
			'privateCpu': {
				'total': privateCpu,
				'wait': privateCpu,
				'idle': 0,
				'running': 0
			},
			'publicCpu': {
				'total': publicCpu,
				'wait': publicCpu,
				'idle': 0,
				'running': 0
			}
		}

	def _addClientObject(self, clientId, clientObject):
		self._clientPool[clientId] = clientObject

	def _delClientObject(self, clientId):
		del self._clientPool[clientId]

	def _clearClientPool(self):
		self._clientPool = {}

	def _getWorkerCpuStatus(self, workerObj):
		workerId = cvtWorkerId(workerObj)
		if self._workerCpuStatus.has_key(workerId):
			return self._workerCpuStatus[workerId]
		else:
			pass

	def _setCurrentPriorityWorker(self, workerId):
		self._currPriorityWorker = workerId

	def _getCurrentPriorityWorker(self):
		return self._currPriorityWorker
