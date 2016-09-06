#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_hdfs import getHdfsInfo
from common.conf_collector import getLocalTmpPath
from hdfs import InsecureClient
import os

class FSHandler:
	def __init__(self, logger):
		self._logger = logger
		hdfsUrl, user = getHdfsInfo()
		self.hdfsClient = InsecureClient(hdfsUrl, user=user)

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
			# self._logger.debug(" - HDFS  : %s" %hdfsFilePath)
			self._logger.debug(" - LOCAL  : %s" %localTempFilePath)
			self.hdfsClient.download(hdfsFilePath, localTempFilePath)
			tmpUsageFiles.append(localTempFilePath)
		return tmpUsageFiles

	def removeFiles(self, localFilePaths):
		try:
			for filePath in localFilePaths:
				# os.remove(filePath)
				pass
		except Exception, e:
			self._logger.exception(e)