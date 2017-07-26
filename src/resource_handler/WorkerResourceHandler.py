#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_system import getPublicCpu, getPrivateCpu, getMaxProcess, getMlGpu
from WorkerResourcePool import WorkerResourcePool
from threading import Lock
import Queue

class WorkerResourceHandler(WorkerResourcePool):
	def __init__(self, logger):
		WorkerResourcePool.__init__(self, logger, DEFAULT_NAME = 'WORKER')
		logger.info("# Set worker resources pool")
		self._lock = Lock()
		self._initPrivateCpu(getPrivateCpu())
		self._initPublicCpu(getPublicCpu())
		self._initMlGpu(getMlGpu())
		self._initMlProcess()

	def getPName(self):
		return self._getPName

	def setUseCpu(self):
		self._lock.acquire()
		self._privateCpu = getPrivateCpu()
		self._publicCpu = getPublicCpu()
		availCpu = self.getAvailCpu()
		self._lock.release()
		return availCpu

	def assignUsePrivateCpu(self):
		self._lock.acquire()
		self._privateCpu -= 1
		availCpu = self.getAvailCpu()
		self._lock.release()
		return availCpu

	def assignUsePublicCpu(self):
		self._lock.acquire()
		self._privateCpu -= 1
		availCpu = self.getAvailCpu()
		self._lock.release()
		return availCpu

	def returnUsePrivateCpu(self):
		self._lock.acquire()
		self._privateCpu += 1
		availCpu = self.getAvailCpu()
		self._lock.release()
		return availCpu

	def returnUsePublicCpu(self):
		self._lock.acquire()
		self._privateCpu += 1
		availCpu = self.getAvailCpu()
		self._lock.release()
		return availCpu

	def getTotalCpu(self):
		return getMaxProcess()

	def getAvailCpu(self):
		availCpu = self._getPrivateCpu() + self._getPublicCpu()
		return availCpu

	def getPrivateCpu(self):
		return self._getPrivateCpu()

	def getPublicCpu(self):
		return self._getPublicCpu()

	def getMlGpu(self):
		return self._getMlGpu()

	def resetAvailCpu(self, resetCore):
		self._lock.acquire()
		availCpu = self._resetAvailCpu(resetCore)
		self._lock.release()
		return availCpu

	def assignCpuByProcessType(self, processType):
		if processType == 'privateCpu':
			self.assignUsePrivateCpu()
		else:
			self.assignUsePublicCpu()

	def returnCpuByProcessType(self, processType):
		if processType == 'privateCpu':
			self.returnUsePrivateCpu()
		else:
			self.returnUsePublicCpu()

	def getMlProcess(self):
		return self._getMlProcess()

	def putMlProcess(self, processType, processIndex):
		self._putMlProcess(processType, processIndex)
