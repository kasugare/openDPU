#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_hdfs import getHdfsInfo
from fs_handler.HaHadoopConnector import HaHadoopConnector
from error_code.ErrorCode import PathNotExistsError, PathExistsError
from requests import ConnectionError
from hdfs import HdfsError
from threading import Lock
import copy
import time
import os

class HdfsHandler(HaHadoopConnector):
	def __init__(self, logger=None, hdfsHosts=None, user=None):
		if not hdfsHosts or not user:
			hdfsHosts, user = getHdfsInfo()
		HaHadoopConnector.__init__(self, logger, hdfsHosts, user)
		self._logger = logger
		self._retryCount = 0

	def getHdfsConn(self):
		return self.getConnection()

	def ls(self, path, status=False):
		hdfsCli = self.getConnection()
		filePathList = [str('%s%s' %(path, fileName)) for fileName in hdfsCli.list(path)]

		if status is not True:
			return filePathList
		else:
			statusFilePathList = []
			for filePath in filePathList:
				fileStatus = hdfsCli.status(filePath)
				statusFilePathList.append({
					'filePath': filePath,
					'group': str(fileStatus['group']),
					'owner': str(fileStatus['owner']),
					'permission': int(fileStatus['permission']),
					'size': str(fileStatus['length']),
					'fileType': str(fileStatus['type'])})
			filePathList = statusFilePathList
		return filePathList

	def exists(self, hdfsPath):
		hdfsCli = self.getConnection()
		exists = False
		try:
			hdfsCli.content(hdfsPath)
			exists = True
		except HdfsError, e:
			self._logger.debug('%s not exists.' %(hdfsPath))
		except ConnectionError, e:
			self._logger.error('Hdfs connection error')
		except Exception, e:
			self._logger.exception(e)
		return exists

	def list(self, hdfsPath):
		hdfsCli = self.getConnection()
		return hdfsCli.list(hdfsPath)

	def status(self, hdfsPath):
		hdfsCli = self.getConnection()
		return hdfsCli.status(hdfsPath)


	def mkdir(self, srcPath, reculsive=False):
		try:
			if self.exists(srcPath):
				raise PathExistsError
			hdfsCli = self.getConnection()
			hdfsCli.makedirs(srcPath)
		except Exception, e:
			self._logger.exception(e)

	def put(self, hdfsDirPath, localFilePath, overwrite=False):
		retryCount = copy.deepcopy(self._retryCount)
		while retryCount < self.getMaxRetryCount():
			try:
				hdfsCli = self.getConnection()
				if retryCount > 0:
					debugMsg = "retryCount : %d" %retryCount
					self._printDebug(debugMsg)
				hdfsFilePath = os.path.join(hdfsDirPath, os.path.basename(localFilePath))
				if self.exists(hdfsFilePath) and retryCount == 0:
					if overwrite:
						hdfsCli.delete(hdfsFilePath)
					else:
						raise PathExistsError
				elif self.exists(hdfsFilePath) and retryCount > 0:
					hdfsCli.delete(hdfsFilePath)
				hdfsCli.upload(hdfsDirPath, localFilePath)
				break
			except PathNotExistsError, e:
				errorMsg = "%s file/dir not exists error : %s, " %(localFilePath, str(e))
				self._printError(errorMsg)
				raise PathNotExistsError
			except PathExistsError, e:
				errorMsg = "%s file/dir not exists error : %s, " %(hdfsFilePath, str(e))
				self._printError(errorMsg)
				raise PathExistsError
			except HdfsError, e:
				errorMsg = "[SAFE MODE] : %s" %(str(e).replace("\n", " "))
				self._printError(errorMsg)
				retryCount += 1
				hasSafeTime, safeTime = self._extractSafeTime(errorMsg)
				if hasSafeTime:
					time.sleep(safeTime)
				else:
					time.sleep(10)
			except ConnectionError, e:
				errorMsg = "connection error : %s" %(str(e))
				self._printError(errorMsg)
				retryCount += 1
				time.sleep(1)
			except Exception, e:
				self._printException(e)
				raise Exception

	def get(self, hdfsFilePath, localFilePath, overwrite=False):
		retryCount = copy.deepcopy(self._retryCount)
		while retryCount < self.getMaxRetryCount():
			try:
				hdfsCli = self.getConnection()
				if retryCount > 0:
					debugMsg = "retryCount : %d" %retryCount
					self._printDebug(debugMsg)
				if not self.exists(hdfsFilePath):
					raise PathNotExistsError
				if os.path.exists(localFilePath):
					if overwrite:
						if os.path.isfile(localFilePath):
							os.remove(localFilePath)
					else:
						raise PathExistsError
				hdfsCli.download(hdfsFilePath, localFilePath)
				break
			except PathNotExistsError, e:
				errorMsg = "%s file/dir not exists error : %s, " %(hdfsFilePath, str(e))
				self._printError(errorMsg)
				raise PathNotExistsError
			except PathExistsError, e:
				errorMsg = "%s file/dir already exists error : %s," %(localFilePath, str(e))
				self._printError(errorMsg)
				raise PathExistsError
			except HdfsError, e:
				errorMsg = "[SAFE MODE] : %s" %(str(e).replace("\n", " "))
				self._printError(errorMsg)
				retryCount += 1
				hasSafeTime, safeTime = self._extractSafeTime(errorMsg)
				if hasSafeTime:
					time.sleep(safeTime)
				else:
					time.sleep(10)
			except ConnectionError, e:
				errorMsg = "connection error : %s" %(str(e))
				self._printError(errorMsg)
				retryCount += 1
				time.sleep(1)
			except Exception, e:
				self._printException(e)
				raise Exception


	def write(self, srcDataSet, hdfsFilePath, linefeed=False, lineflush=False):
		retryCount = copy.deepcopy(self._retryCount)
		while retryCount < self.getMaxRetryCount():
			try:
				hdfsCli = self.getConnection()
				if retryCount > 0:
					debugMsg = "retryCount : %d" %retryCount
					self._printDebug(debugMsg)
				if self.exists(hdfsFilePath):
					hdfsCli.delete(hdfsFilePath)

				with hdfsCli.write(hdfsFilePath) as writer:
					if lineflush:
						for data in srcDataSet:
							if linefeed:
								writer.write("%s\n" %data)
							else:
								writer.write("%s" %data)
					else:
						if type(srcDataSet) == list:
							strDataSet = "\n".join(srcDataSet) if linefeed else "".join(srcDataSet)# + "\n"
						elif type(srcDataSet) == str:
							strDataSet = "%s\n" %(srcDataSet) if linefeed else "%s" %(srcDataSet)
						else:
							strDataSet = "%s\n" %(str(srcDataSet)) if linefeed else "%s" %(str(srcDataSet))
						writer.write(strDataSet)
				break
			except HdfsError, e:
				errorMsg = "[SAFE MODE] : %s" %(str(e).replace("\n", " "))
				self._printError(errorMsg)
				retryCount += 1
				hasSafeTime, safeTime = self._extractSafeTime(errorMsg)
				if hasSafeTime:
					time.sleep(safeTime)
				else:
					time.sleep(10)
			except ConnectionError, e:
				errorMsg = "connection error : %s" %(str(e))
				self._printError(errorMsg)
				retryCount += 1
				time.sleep(1)
			except Exception, e:
				self._printException(e)
				raise Exception

	def read(self, hdfsFilePath):
		data = None
		retryCount = copy.deepcopy(self._retryCount)

		while retryCount < self.getMaxRetryCount():
			try:
				hdfsCli = self.getConnection()
				if retryCount > 0:
					debugMsg = "retryCount : %d" %retryCount
					self._printDebug(debugMsg)

				with hdfsCli.read(hdfsFilePath) as reader:
					data = reader.read()
				break
			except HdfsError, e:
				errorMsg = "[SAFE MODE] : %s" %(str(e).replace("\n", " "))
				self._printError(errorMsg)
				retryCount += 1
				hasSafeTime, safeTime = self._extractSafeTime(errorMsg)
				if hasSafeTime:
					time.sleep(safeTime)
				else:
					time.sleep(10)
			except ConnectionError, e:
				errorMsg = "connection error : %s" %(str(e))
				self._printError(errorMsg)
				retryCount += 1
				time.sleep(1)
			except Exception, e:
				self._printException(errorMsg)
				raise Exception
		return data
		
	def readlines(self, hdfsFilePath):
		dataSet = None
		retryCount = copy.deepcopy(self._retryCount)

		while retryCount < self.getMaxRetryCount():
			try:
				hdfsCli = self.getConnection()
				if retryCount > 0:
					debugMsg = "retryCount : %d" %retryCount
					self._printDebug(debugMsg)

				with hdfsCli.read(hdfsFilePath) as reader:
					dataSet = reader.read().split('\n')
				break
			except HdfsError, e:
				errorMsg = "[SAFE MODE] : %s" %(str(e).replace("\n", " "))
				self._printError(errorMsg)
				retryCount += 1
				hasSafeTime, safeTime = self._extractSafeTime(errorMsg)
				if hasSafeTime:
					time.sleep(safeTime)
				else:
					time.sleep(10)
			except ConnectionError, e:
				errorMsg = "connection error : %s" %(str(e))
				self._printError(errorMsg)
				retryCount += 1
				time.sleep(1)
			except Exception, e:
				self._printException(errorMsg)
				raise Exception
		return dataSet

	def existHdfsFiles(self, hdfsPath, hdfsFileName):
		try:
			hdfsCli = self.getConnection()
			fileList = hdfsCli.list(hdfsPath)
			for fileName in fileList:
				if hdfsFileName == fileName.split('_')[0]:
					hdfsFilePath = '%s/%s' %(hdfsPath, fileName)
					hdfsFileSize = hdfsCli.status(hdfsFilePath)['length']
					return hdfsFilePath, hdfsFileSize
		except Exception, e:
			self._logger.warn('# File %s/%s does not exist.' %(hdfsPath, hdfsFileName))
		return None, None

