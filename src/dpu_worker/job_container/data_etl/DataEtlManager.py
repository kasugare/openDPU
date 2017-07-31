#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from fs_handler.FSHandler import FSHandler
import os

PROCESS_NAME = 'DATA_ETL'

class DataEtlManager:
	def __init__(self, logger=None, jobId=None, debugMode='DEBUG'):
		if logger:
			self._logger = logger
		else:
			self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.setLevel(debugMode)
		self._fsHandler = FSHandler(logger)
		self._hdfsClient = self._fsHandler.getHdfsClient()
		self._jobId = jobId

	def doProcess(self, **args):
		self._logger.info("###########################")
		self._logger.info("### DO Process Data Etl ###")
		self._logger.info("###########################")

		storageType = args['storageType']
		filePath = args['filePath']
		fileName = ('.').join(os.path.basename(filePath).split('.')[:-1])
		byteSeq = self.__loadSequenceByteData(storageType, filePath)

		gramDict = self.__cvtNgram(byteSeq, 2)
		resultGramList = [fileName]

		for gramKey in gramDict.keys():
			resultGramList.append('%s:%d' %(gramKey, gramDict[gramKey]))

		reusltFilePath = self.__saveSequenceByteData(storageType, filePath, resultGramList)
		return reusltFilePath

	def __cvtNgram(self, byteSeq, N):
		gramDict = dict()
		index = 0
		for index in range(len(byteSeq)):
			gram = ''.join(byteSeq[index:index+N])
			if gram in gramDict:
				gramDict[gram] += 1
			else:
				gramDict[gram] = 1
			index+= 1
		return gramDict

	def __loadSequenceByteData(self, storageType, filePath):
		# Remove memory address & concatenate bytes
		byteSeq = list()
		if storageType == 'HDFS':
			rawData = self._hdfsClient.readlines(filePath)
			for rawByte in rawData:
				byteSeq += rawByte.split()[1:]
		elif storageType == 'FILE':
			for rawByte in open(filePath):
				byteSeq += rawByte.split()[1:]
		return byteSeq

	def __saveSequenceByteData(self, storageType, filePath, resultGramList):
		fileName = 'temp_%s.txt' %('.').join(os.path.basename(filePath).split('.')[:-1])
		gramData = ' '.join(resultGramList)

		if storageType == 'HDFS':
			tempDirPath = '/'.join(filePath.split('/')[:-2] + ['temp'] + [self._jobId])
			if not self._fsHandler.hdfsExists(tempDirPath):
				self._hdfsClient.mkdir(tempDirPath)
			tempFilePath = os.path.join(tempDirPath, fileName)
			self._hdfsClient.write(gramData, tempFilePath, linefeed=True)

		elif storageType == 'FILE':
			tempDirPath = '/'.join(filePath.split('/')[:-2] + ['temp'] + [self._jobId])
			if not os.path.exists(tempDirPath):
				os.makedirs(tempDirPath)
			tempFilePath = os.path.join(tempDirPath, fileName)
			fd = open(tempFilePath, 'w')
			fd.write(gramData + "\n")
			fd.flush()
			fd.close()

		return tempFilePath