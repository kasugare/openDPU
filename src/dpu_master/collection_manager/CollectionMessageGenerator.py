#!/usr/bin/env python
# -*- coding: utf-8 -*-

from protocol.message_pool.MessageGenerator import getReqEtlJob
import time
import os

class CollectionMessageGenerator:
	def __init__(self, logger):
		self._logger = logger

	def genCollectionMessages(self, userParams):
		jobs = userParams['jobs']
		storageType = userParams['storage']
		paths = userParams['paths']
		filePathList = []
		protoMsgs = []
		jobId = '%s_%d' %(jobs, time.time() * 1000000)

		for targetPath in paths:
			if storageType == 'FILE':
				if os.path.exists(targetPath):
					if os.path.isdir(targetPath):
						filePathList += self._getFilePathInDir(targetPath)
					else:
						filePathList.append(targetPath)

			elif storageType == 'HDFS':
				pass
		filePathList = list(set(filePathList))

		for index in range(len(filePathList)):
			params = {
				'storageType': storageType,
				'filePath': filePathList[index]
			}
			protoMsgs.append(getReqEtlJob(jobType=jobs, jobId=jobId, taskId=index, params=params))

		self._printMsgParams(protoMsgs)
		return jobId, protoMsgs

	def _getFilePathInDir(self, dirPath):
		filePathList = []
		for targetName in os.listdir(dirPath):
			targetPath = os.path.join(dirPath, targetName)
			if os.path.isfile(targetPath):
				filePathList.append(targetPath)
			else:
				filePathList += self._getFilePathInDir(targetPath)
		return filePathList

	def _printMsgParams(self, protoMsgs):
		self._logger.info('-' * 71)
		for protoMsg in protoMsgs:
			self._logger.debug(' - %s' %protoMsg)
		self._logger.info('-' * 71)