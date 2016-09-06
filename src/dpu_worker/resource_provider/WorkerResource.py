#!/usr/bin/env python
# -*- coding: utf-8 -*-

class WorkerResource:
	def __init__(self, logger):
		self._logger = logger
		self._workerID = None
		self._workerResource = {}
		self._initResource()
		print self._workerResource

	def _initResource(self):
		self.addAppResource('NILM', 5, 0, 5)
		self.addAppResource('COLLECTOR', 1, 0, 1)

	def setWorkerID(self, workerID):
		try:
			self._workerID = workerID
		except Exception, e:
			self._logger.exception(e)
			return False
		finally:
			return True

	def getWorkerID(self):
		return self._workerID

	def getCurrentResource(self):
		return self._workerResource

	def getAppResource(self, appName):
		if self._workerResource.has_key(appName):
			return {appName : self._workerResource[appName]}

	def delAppResource(self, appName):
		try:
			if self._workerResource.has_key(appName):
				del self._workerResource[appName]
		except Exception, e:
			self._logger.exception(e)
			return False
		finally:
			return True

	def addAppResource(self, appName, total, running, idle):
		try:
			if self._workerResource.has_key(appName):
				raise AssertionError
			if not total == (running + idle) or total < 0:
				raise ValueError

			self._workerResource[appName] = {
				'total': total,
				'running': running,
				'idle': idle
			}
		except Exception, e:
			self._logger.exception(e)
			return False
		finally:
			return True

	def clearResource(self):
		try:
			self._workerResource.clear()
		except Exception, e:
			self._logger.exception(e)
			return False
		finally:
			return True