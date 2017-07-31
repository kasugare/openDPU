#!/usr/bin/env python
# -*- coding: utf-8 -*-

from protocol.message_pool.MessageGenerator import getReqEtlJob
from fs_handler.FSHandler import FSHandler
import time
import os

class CollectionMessageGenerator:
	def __init__(self, logger):
		self._logger = logger
		self._fsHandler = FSHandler(logger)

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
						filePathList += self._fsHandler.getLocalFileList(targetPath, recursive=True)
					else:
						filePathList.append(targetPath)

			elif storageType == 'HDFS':
				if self._fsHandler.hdfsExists(targetPath):
					if self._fsHandler.isdir(targetPath):
						filePathList += self._fsHandler.getHdfsFileList(targetPath, recursive=True)
					else:
						filePathList.append(targetPath)

		filePathList = list(set(filePathList))
		filePathList.sort()

		for index in range(len(filePathList)):
			params = {
				'storageType': storageType,
				'filePath': filePathList[index]
			}
			protoMsgs.append(getReqEtlJob(jobType=jobs, jobId=jobId, taskId=index, params=params, processType='MAP'))

		self._printMsgParams(protoMsgs)
		return jobId, protoMsgs

	def _printMsgParams(self, protoMsgs):
		self._logger.info('-' * 71)
		for protoMsg in protoMsgs:
			self._logger.debug(' - %s' %protoMsg)
		self._logger.info('-' * 71)