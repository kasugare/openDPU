#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_hdfs import getHdfsInfo
from common.conf_collector import getLocalTmpPath
from fs_handler.HdfsHandler import HdfsHandler
from hdfs import InsecureClient
from threading import Lock
import random
import time
import json
import os

class FSHandler:
	def __init__(self, logger):
		self._logger = logger
		hdfsHosts, user = getHdfsInfo()
		self._hdfsClient = HdfsHandler(logger, hdfsHosts, user)
		self._lock = Lock()

	def _getUniqTimeNum(self):
		self._lock.acquire()
		uniqTimeNum = int('%d%d' %(int(time.time()*100)+random.randrange(1,9), random.randrange(1,9)))
		self._lock.release()
		return uniqTimeNum

	def getHdfsClient(self):
		return self._hdfsClient

	def getHdfsFileToLocal(self, hdfsFilePath, localDirPath):
		self._hdfsClient.get(hdfsFilePath, localDirPath)

	def getHdfsFilesForMeta(self, startTS, endTS, hdfsFilePaths):
		def _mergeFiles(startTS, endTS, tmpLocalFiles):
			if not tmpLocalFiles:
				return
			tmpLocalFiles.sort()
			sid_did_fid = None
			if tmpLocalFiles:
				sid_did_fid = tmpLocalFiles[0].split('.')[1]
			else:
				return

			mergedFilePath = '%s/%d-%d.%s.csv' %(getLocalTmpPath(), startTS, endTS, sid_did_fid)
			try:
				writer = open(mergedFilePath, 'w')
				for filePath in tmpLocalFiles:
					reader = open(filePath, 'r')
					dataList = reader.readlines()
					for data in dataList:
						data = '%s\n' %(data.replace('\n',''))
						writer.write(data)
					reader.close()
					os.remove(filePath)
				if writer: writer.close()
			except Exception, e:
				self._logger.error(e)
				if writer: writer.close()
				return None
			return mergedFilePath

		tmpUsageFiles = self.getHdfsFileForUsage(hdfsFilePaths)
		tmpMetaFile = _mergeFiles(startTS, endTS, tmpUsageFiles)
		self._logger.debug(" - Merged : %s" %tmpMetaFile)
		return [tmpMetaFile]

	def getHdfsFileForUsage(self, hdfsFilePaths):
		tmpLocalPath = getLocalTmpPath()
		tmpUsageFiles = []
		for hdfsFilePath in hdfsFilePaths:
			localTempFilePath = '%s/%s' %(tmpLocalPath, hdfsFilePath.split('/')[-1])
			if os.path.exists(localTempFilePath):
				os.remove(localTempFilePath)
			self._logger.debug(" - %s -> %s" %(hdfsFilePath, localTempFilePath))
			try:
				self._hdfsClient.get(hdfsFilePath, localTempFilePath, overwrite=True)
				tmpUsageFiles.append(localTempFilePath)
			except Exception, e:
				self._logger.exception(e)
		return tmpUsageFiles

	def saveDpuMetaInfoToFile(self, fileName, metaInfo):
		tmpLocalPath = getLocalTmpPath()
		fileName = os.path.basename(fileName)
		metaFileName = 'meta_%s_%d.json' %(fileName.split('.')[1], self._getUniqTimeNum())
		localTempFilePath = '%s/%s' %(tmpLocalPath, metaFileName)
		self._logger.debug('# Save dpu meta info to file for dpu usage. file path : %s' %(localTempFilePath))
		try:
			fd = open(localTempFilePath, 'w')
			fd.write(json.dumps(metaInfo))
			fd.flush()
			fd.close()
		except Exception, e:
			self._logger.exception(e)
		return localTempFilePath

	def removeFiles(self, localFilePaths):
		try:
			for filePath in localFilePaths:
				os.remove(filePath)
		except Exception, e:
			self._logger.exception(e)

	def mkdirOnLocal(self, localDirPath):
		try:
			os.mkdir(localDirPath)
		except Exception, e:
			self._logger.exception(e)
			return Noe
		return localDirPath

	def getLocalFileList(self, localDirPath, recursive=False):
		filePathList = []
		for targetName in os.listdir(localDirPath):
			targetPath = os.path.join(localDirPath, targetName)
			if os.path.isfile(targetPath):
				filePathList.append(targetPath)
			else:
				filePathList += self.getLocalFileList(targetPath)
		return filePathList

	def getHdfsFileList(self, hdfsPath, recursive=False):
		filePathList = []
		for targetPath in [str(dirPath['filePath']) for dirPath in self._hdfsClient.ls(hdfsPath, status=True)]:
			if self._hdfsClient.isfile(targetPath):
				filePathList.append(targetPath)
			elif recursive:
				filePathList += self.getHdfsFileList(targetPath)
		return filePathList

	def hdfsExists(self, hdfsPath):
		return self._hdfsClient.exists(hdfsPath)

	def isdir(self, hdfsPath):
		return self._hdfsClient.isdir(hdfsPath)
