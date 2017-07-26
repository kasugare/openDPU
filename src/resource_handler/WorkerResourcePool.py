#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_common import cvtWorkerId
from threading import Lock
import multiprocessing
import os

class WorkerResourcePool:
	def __init__(self, logger, DEFAULT_NAME):
		logger.info("# Created Resource Pool")
		self._logger = logger
		self._innerResourceQ = multiprocessing.Queue()
		self._mlProcessPool = {}

		self._currPriorityWorker = None
		self._prevWorkerObjs = {}
		self._privateCpu = 0
		self._publicCpu = 0
		self._mlGpu = 0
		
		self._totalCpu = self._privateCpu + self._publicCpu
		self._pName = '%s_%d' %(DEFAULT_NAME, os.getpid())

	def _getPName(self):
		return self._pName

	def _addPrivateCpu(self):
		self._privateCpu += 1
		if self._privateCpu > self._totalCpu or self._privateCpu < 0:
			self._logger.warn("# wrong private cpu core : %d" %self._privateCpu)
		return self._privateCpu

	def _initPrivateCpu(self, privateCpu):
		self._privateCpu = privateCpu

	def _initPublicCpu(self, publicCpu):
		self._publicCpu = publicCpu

	def _initMlGpu(self, mlGpu):
		self._mlGpu = mlGpu

	def _initMlProcess(self):
		def setInitMlProcess(processType, index):
			resource = {
				'processType': processType,
				'processIndex': processIndex
			}
			self._innerResourceQ.put_nowait(resource)

		if self._mlGpu == 0:
			processType = 'CPU'
			maxProcess = self._privateCpu
		else:
			processType = 'GPU'
			maxProcess = self._mlGpu

		self._mlProcessPool[processType] = {
			'maxProcess': maxProcess,
			'activeProcess':0,
			'standbyProcess': 0
		}

		for processType in self._mlProcessPool.keys():
			for processIndex in range(self._mlProcessPool[processType]['maxProcess']):
				self._mlProcessPool[processType]['standbyProcess'] += 1
				setInitMlProcess(processType, processIndex)

	def _getTotalCpu(self):
		return self._totalCpu

	def _getPrivateCpu(self):
		return self._privateCpu

	def _getPublicCpu(self):
		return self._publicCpu

	def _getMlGpu(self):
		return self._mlGpu

	def _getMlProcess(self):
		mlProcessInfo = self._innerResourceQ.get()
		processType = mlProcessInfo['processType']
		if self._mlProcessPool.has_key(processType):
			maxProcess = self._mlProcessPool[processType]['maxProcess']
			activeProcess = self._mlProcessPool[processType]['activeProcess']
			standbyProcess = self._mlProcessPool[processType]['standbyProcess']

			if maxProcess == activeProcess + standbyProcess:
				self._mlProcessPool[processType]['activeProcess'] += 1
				self._mlProcessPool[processType]['standbyProcess'] -= 1
			else:
				self._logger.warn("# total ml process is not same to sum active with standby processes")
		self._print()
		return mlProcessInfo

	def _putMlProcess(self, processType, processIndex):
		resource = {
			'processType': processType,
			'processIndex': processIndex
		}
		self._innerResourceQ.put_nowait(resource)
		
		if self._mlProcessPool.has_key(processType):
			maxProcess = self._mlProcessPool[processType]['maxProcess']
			activeProcess = self._mlProcessPool[processType]['activeProcess']
			standbyProcess = self._mlProcessPool[processType]['standbyProcess']

			if maxProcess == activeProcess + standbyProcess:
				self._mlProcessPool[processType]['activeProcess'] -= 1
				self._mlProcessPool[processType]['standbyProcess'] += 1
			else:
				self._logger.warn("# total ml process is not same to sum active with standby processes")
		self._print()

	def _print(self):
		for processType in self._mlProcessPool.keys():
			maxProcess = self._mlProcessPool[processType]['maxProcess']
			activeProcess = self._mlProcessPool[processType]['activeProcess']
			standbyProcess = self._mlProcessPool[processType]['standbyProcess']

			self._logger.info("DL INFO : [%s] max_process : %d, act_process : %d, std_process : %d" %(processType, maxProcess, activeProcess, standbyProcess))

