#!/usr/bin/env python
# -*- coding: utf-8 -*-

from protocol.message_pool.MessageGenerator import genReqNilmData
from common.util_datetime import convertOneDayBaseTimestampGmt9
from FSHandler import FSHandler
import time
import sys
import os

class NilmRunner(FSHandler):
	def __init__(self, logger, jobId, outputQ, sender):
		FSHandler.__init__(self, logger)
		self._logger = logger
		self._jobId = jobId
		self._outputQ = outputQ
		self._sender = sender

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
		# self._logger.debug(params)

		sid = params['sid']
		did = params['did']
		lfid = params['lfid']
		freq = params['freq']
		startTS = params['startTS']
		endTS = params['endTS']
		analysisType = params['analysisType']
		hdfsFilePaths = params['filePath']

		tmpMetaFiles = self.getHdfsFilesForMeta(startTS, endTS, hdfsFilePaths)

		from NilmMetaSearch import NilmMetaSearch
		for tmpMetaFile in tmpMetaFiles:
			dailyBasedTS = convertOneDayBaseTimestampGmt9(startTS)
			nilmCore = NilmMetaSearch(self._logger, self._jobId, analysisType)
			resultMap = nilmCore.doProcess(freq, sid, did, lfid, tmpMetaFile, dailyBasedTS)
		self.removeFiles(tmpMetaFiles)

		for tmpUsageFile in hdfsFilePaths:
			params['filePath'] = [tmpUsageFile]
			splitedTS = tmpUsageFile.split('/')[-1].split('.')[0].split('-')
			params['startTS'] = int(splitedTS[0])
			params['endTS'] = int(splitedTS[1])
			params['analysisType'] = 'usage'
			self._sender.sendMsg(genReqNilmData('tajo', 'edm3', 'NILM', 'HDFS', params))
			time.sleep(0.1)
		# return resultMap

	def _doMetaUpdate(self, params):
		self._logger.info("###################################")
		self._logger.info("### DO Process Nilm Meta Update ###")
		self._logger.info("###################################")
		# self._logger.debug(params)

		sid = params['sid']
		did = params['did']
		lfid = params['lfid']
		freq = params['freq']
		startTS = params['startTS']
		endTS = params['endTS']
		analysisType = params['analysisType']
		hdfsFilePaths = params['filePath']

		tmpMetaFiles = self.getHdfsFilesForMeta(startTS, endTS, hdfsFilePaths)

		from NilmMetaUpdate import NilmMetaUpdate
		for tmpMetaFile in tmpMetaFiles:
			dailyBasedTS = convertOneDayBaseTimestampGmt9(startTS)
			nilmCore = NilmMetaUpdate(self._logger, self._jobId, analysisType)
			resultMap = nilmCore.doProcess(freq, sid, did, lfid, tmpMetaFile, dailyBasedTS)
		self.removeFiles(tmpMetaFiles)
		# return resultMap

	def _doAppUsage(self, params):
		self._logger.info("#############################")
		self._logger.info("### DO Process Nilm Usage ###")
		self._logger.info("#############################")
		# self._logger.debug(params)

		sid = params['sid']
		did = params['did']
		lfid = params['lfid']
		freq = params['freq']
		startTS = params['startTS']
		analysisType = params['analysisType']
		hdfsFilePaths = params['filePath']

		tmpUsageFiles = self.getHdfsFileForUsage(hdfsFilePaths)

		from NilmAppUsage import NilmAppUsage
		for tmpUsageFile in tmpUsageFiles:
			dailyBasedTS = convertOneDayBaseTimestampGmt9(startTS)
			nilmCore = NilmAppUsage(self._logger, self._jobId, analysisType.upper())
			resultMap = nilmCore.doProcess(freq, sid, did, lfid, tmpUsageFile, dailyBasedTS)
			if resultMap:
				self._outputQ.put_nowait(resultMap)
		self.removeFiles(tmpUsageFiles)
		# return resultMap
