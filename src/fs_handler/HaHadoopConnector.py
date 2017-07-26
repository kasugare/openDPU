#!/usr/bin/env python
# -*- coding: utf-8 -*-

from hdfs import InsecureClient, HdfsError
from requests import ConnectionError
from threading import Lock
import traceback
import time

class HaHadoopConnector:
	def __init__(self, logger, hdfsHosts, user):
		self.logger = logger
		self._hdfsHosts = hdfsHosts
		self._user = user
		self._hdfsCli = None
		self._connHdfsInfo = None
		self._maxRetry = 10
		self._lock = Lock()

	def close(self):
		self._hdfsCli = None
		self._connHdfsInfo = None

	def _printDebug(self, message):
		if self.logger:
			self.logger.debug(message)
		else:
			print message

	def _printError(self, errorMsg):
		if self.logger:
			self.logger.warn(errorMsg)
		else:
			print "[warn] %s" %(errorMsg)

	def _printException(self, exception):
		if self.logger:
			self.logger.exception(exception)
		else:
			print traceback.format_exc(exception)

	def _extractSafeTime(self, message):
		splitedErrorMsg = message.split(' ')
		safeTime = splitedErrorMsg[len(splitedErrorMsg)-2]
		if safeTime.isdigit():
			return True, int(safeTime) + 5
		else:
			return False, None

	def _setConnection(self):
		if self._connHdfsInfo and self._hdfsCli:
			return self._hdfsCli

		self._lock.acquire()
		for hdfsHost in self._hdfsHosts:
			try:
				self._hdfsCli = InsecureClient(hdfsHost, user=self._user)
				self._hdfsCli.status('/')
				self._connHdfsInfo = hdfsHost
				debugMsg = "connected hdfs : %s" %hdfsHost
				if self.logger:
					self.logger.debug(debugMsg)
				break
			except HdfsError, e:
				self.close()
				errorMsg = "hdfs error : %s, %s" %(str(e), hdfsHost)
				self._printError(errorMsg)
			except ConnectionError, e:
				self.close()
				errorMsg = "connection error : %s, %s" %(str(e), hdfsHost)
				time.sleep(1)
				self._printError(errorMsg)
			except Exception, e:
				self.close()
				errorMsg = "connection error : %s" %(hdfsHost)
				self._printError(errorMsg)
				self._printException(e)
				if self._lock:
					self._lock.release()
				raise Exception
		self._lock.release()
		return self._hdfsCli

	def getConnection(self):
		retryCount = 0
		while retryCount < self._maxRetry:
			try:
				connHdfs = self._setConnection()
				if connHdfs:
					self._hdfsCli.content('/')
					break
				else:
					retryCount += 1
			except HdfsError, e:
				errorMsg = "hdfs error : %s" %(str(self.getConnectedHdfsInfo()))
				self._printError(errorMsg)
				self.close()
				retryCount += 1
			except ConnectionError, e:
				errorMsg = "connection error : %s" %(str(self.getConnectedHdfsInfo()))
				self._printError(errorMsg)
				self.close()
				retryCount += 1
			except Exception, e:
				self.close()
				errorMsg = "exception : %s" %(str(self.getConnectedHdfsInfo()))
				self._printError(errorMsg)
				self._printException(e)
				raise Exception
		return self._hdfsCli

	def getMaxRetryCount(self):
		return self._maxRetry

	def getConnectedHdfsInfo(self):
		return self._connHdfsInfo

