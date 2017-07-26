#!/usr/bin/env python
# -*- coding: utf-8 -*-

from protocol.message_pool.MessageGenerator import genReqNilmData
from common.util_datetime import convertOneDayBaseTimestampGmt9
from fs_handler.FSHandler import FSHandler
import time
import sys
import os

class NilmRunner:
	def __init__(self, logger, rLogger, jobId, outputQ, sendMessageQ):
		self._logger = logger
		self._rLogger = rLogger
		self._jobId = jobId
		self._outputQ = outputQ
		self._sendMessageQ = sendMessageQ
		self._fsHandler = FSHandler(logger)

	def doProcess(self, params):
		analysisType = params['analysisType']
		if analysisType == 'metaSearch':
			resultMap = self._doMetaSearch(params)
			return resultMap
		elif analysisType == 'metaUpdate':
			resultMap = self._doMetaUpdate(params)
			return resultMap
		elif analysisType == 'usage':
			resultMap = self._doAppUsage(params)
			return resultMap
		self._logger.info("# Done nilm core process")

	def _doMetaSearch(self, params):
		self._logger.info("###################################")
		self._logger.info("### DO Process Nilm Meta Search ###")
		self._logger.info("###################################")
		self._logger.debug(params)

		sid = params['sid']
		did = params['did']
		lfid = params['lfid']
		startTS = params['startTS']
		endTS = params['endTS']
		analysisType = params['analysisType']
		hdfsFilePaths = params['filePath']
		countryCode = params['country']
		timezone = params['timezone']
		debugMode = params['debugMode']

		tmpMetaFiles = self._fsHandler.getHdfsFilesForMeta(startTS, endTS, hdfsFilePaths)

		from NilmMetaLearner import NilmMetaLearner
		for tmpMetaFile in tmpMetaFiles:
			dailyBasedTS = convertOneDayBaseTimestampGmt9(startTS)
			nilmCore = NilmMetaLearner(self._logger, self._rLogger, self._jobId, analysisType, countryCode, timezone, self._fsHandler, debugMode)
			resultMap = nilmCore.doProcess(sid, did, lfid, tmpMetaFile, dailyBasedTS)

		# for tmpUsageFile in hdfsFilePaths:
		# 	params['filePath'] = [tmpUsageFile]
		# 	splitedTS = tmpUsageFile.split('/')[-1].split('.')[0].split('-')
		# 	params['startTS'] = int(splitedTS[0])
		# 	params['endTS'] = int(splitedTS[1])
		# 	params['analysisType'] = 'usage'
		# 	self._sendMessageQ.put_nowait(genReqNilmData('tajo', 'edm3', 'NILM', 'HDFS', params))
		self._fsHandler.removeFiles(tmpMetaFiles)
		

	def _doMetaUpdate(self, params):
		self._logger.info("###################################")
		self._logger.info("### DO Process Nilm Meta Update ###")
		self._logger.info("###################################")
		self._logger.debug(params)

		sid = params['sid']
		did = params['did']
		lfid = params['lfid']
		startTS = params['startTS']
		endTS = params['endTS']
		analysisType = params['analysisType']
		countryCode = params['country']
		timezone = params['timezone']
		debugMode = params['debugMode']
		hdfsFilePaths = params['filePath']
		hdfsFilePaths.sort()

		tmpMetaFiles = self._fsHandler.getHdfsFilesForMeta(startTS, endTS, hdfsFilePaths)

		from NilmMetaUpdateLearner import NilmMetaUpdateLearner
		for tmpMetaFile in tmpMetaFiles:
			dailyBasedTS = convertOneDayBaseTimestampGmt9(startTS)
			nilmCore = NilmMetaUpdateLearner(self._logger, self._rLogger, self._jobId, analysisType, countryCode, timezone, self._fsHandler, debugMode)
			resultMap = nilmCore.doProcess(sid, did, lfid, tmpMetaFile, dailyBasedTS)
		self._fsHandler.removeFiles(tmpMetaFiles)


	def _doAppUsage(self, params):
		self._logger.info("#############################")
		self._logger.info("### DO Process Nilm Usage ###")
		self._logger.info("#############################")
		self._logger.debug(params)

		sid = params['sid']
		did = params['did']
		lfid = params['lfid']
		startTS = params['startTS']
		analysisType = params['analysisType']
		hdfsFilePaths = params['filePath']
		countryCode = params['country']
		timezone = params['timezone']
		rExtOpt = params['rExtOpt']
		debugMode = params['debugMode']

		tmpUsageFiles = self._fsHandler.getHdfsFileForUsage(hdfsFilePaths)

		from NilmUsagePredictor import NilmUsagePredictor
		for tmpUsageFile in tmpUsageFiles:
			if os.path.exists(tmpUsageFile) and os.path.getsize(tmpUsageFile) > 0:
				dailyBasedTS = convertOneDayBaseTimestampGmt9(startTS)
				nilmCore = NilmUsagePredictor(self._logger, self._rLogger, self._jobId, analysisType.upper(), countryCode, timezone, self._fsHandler, debugMode)
				resultMap = nilmCore.doProcess(sid, did, lfid, tmpUsageFile, dailyBasedTS=dailyBasedTS, rExtOpt=rExtOpt)
				if resultMap:
					self._outputQ.put_nowait(resultMap)
				else:
					self._logger.warn('# has not usage output data, sid : %d, country : %s, TZ : %s, hdfs file : %s' %(sid, countryCode, timezone, hdfsFilePaths))
			else:
				self._logger.warn('# empty raw data, sid : %d, file path : %s' %(sid, tmpUsageFile))
		self._fsHandler.removeFiles(tmpUsageFiles)
