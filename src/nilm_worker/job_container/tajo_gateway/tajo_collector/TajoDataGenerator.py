#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_hdfs import getHdfsNilmPath, getHdfsInfo
from common.conf_system import getAllowedMinPeriod
from common.util_common import cvtNumFormat
from common.util_datetime import convertTS2Date, getDailyPeriodSet
from protocol.message_pool.MessageGenerator import genResTajoData, genReqTajoEnable
from nilm_worker.job_container.tajo_gateway.tajo_collector.TajoQueryGenerator import TajoQueryGenerator
from nilm_worker.job_container.tajo_gateway.tajo_collector.TajoDataAggregator import TajoDataAggregator
from nilm_worker.job_container.tajo_gateway.tajo_collector.TajoQueryExecutor import TajoQueryExecutor
from fs_handler.HdfsHandler import HdfsHandler
import copy
import time
import re
import os

class TajoDataGenerator:
	def __init__(self, logger, sendMessageQ):
		self._logger = logger
		self._sendMessageQ = sendMessageQ
		hdfsHosts, user = getHdfsInfo()
		self._hdfsClient = HdfsHandler(logger, hdfsHosts, user)
		self._sendCount = 0

	def doProcess(self, jobType, message):
		self._logger.info('# 1. Get message')
		self._jobType = jobType
		self._offerType = message['offer']
		params = message['params']
		analysisType = params['analysisType']
		self._logger.debug(" - job type : %s, (%d) %s" %(analysisType, len(params['target'].keys()), params['target'].keys()))

		try:
			if analysisType == 'metaSearch' or analysisType == 'metaUpdate':
				self._metaSearch(params)
				self._sendMessageQ.put_nowait(genReqTajoEnable(jobType))
			elif analysisType == 'usage':
				self._usageSearch(params)
			elif analysisType == 'candidate':
				self._usageSearch(params)
		except Exception, e:
			self._sendMessageQ.put_nowait(genReqTajoEnable(jobType))
			self._logger.exception(e)
		self._logger.debug("# [Tajo] Data Collection Done.")

	def _getParamElements(self, params):
		analysisType = params['analysisType']
		startTS = params['startTS']
		endTS = params['endTS']
		siteInfo = params['target']
		deviceCh = params['deviceCh']
		nilmFreq = params['nilmFreq']
		countryCode = params['country']
		timezone = params['timezone']
		rExtOpt = params['rExtOpt']
		debugMode = params['debugMode']
		return analysisType, startTS, endTS, siteInfo, deviceCh, nilmFreq, countryCode, timezone, rExtOpt, debugMode

	def _metaSearch(self, params):
		self._logger.info('# 2. Arrange period by each Site ID')
		analysisType, startTS, endTS, siteInfo, _, _, countryCode, timezone, _, debugMode = self._getParamElements(params)

		sids = siteInfo.keys()
		sidPeriodMap = self._getArrangedSidPeriodMap(startTS, endTS, sids)
		self._logger.debug(" - target sid : %s" %(str(sids)))

		self._logger.info('# 3. Generate File Path')
		sidHdfsFilePathMap = self._genHdfsFilePath(sidPeriodMap, countryCode, timezone)

		def searchFileOfTheSeries(allowedMinPeriod, hdfsFileMap):
			periodKeys = hdfsFileMap.keys()
			keysGroup = []
			sequencedKeys = []
			for key in periodKeys:
				if sequencedKeys:
					if key - sequencedKeys[-1] == 1:
						sequencedKeys.append(key)
					else:
						keysGroup.append(sequencedKeys)
						sequencedKeys = [key]
				else:
					sequencedKeys = [key]
			if sequencedKeys:
				keysGroup.append(sequencedKeys)

			maxCandidate = []
			for countinuousGroup in keysGroup:
				if len(countinuousGroup) >= len(maxCandidate) and len(countinuousGroup) >= allowedMinPeriod:
					maxCandidate = countinuousGroup
			hdfsFileList = [hdfsFileMap[key] for key in maxCandidate]
			return hdfsFileList

		self._logger.info('# 4. Check existed files')
		for sid in sidHdfsFilePathMap.keys():
			hdfsFileList = []
			hdfsFileMap = {}
			did = None
			index = 0
			fileNames = sidHdfsFilePathMap[sid].keys()
			fileNames.sort()
			for fileName in fileNames:
				filePath = sidHdfsFilePathMap[sid][fileName]
				hdfsFilePath, hdfsFileSize = self._hdfsClient.existHdfsFiles(filePath, fileName)
				if hdfsFilePath and hdfsFileSize > 1:
					hdfsFileName = os.path.basename(hdfsFilePath)
					did = int(hdfsFileName.split('_')[1])
					hdfsFileMap[index] = hdfsFilePath
				else:
					if hdfsFileSize == 0:
						self._logger.warn(" - %s is empty data, data size : %d" %(fileName, hdfsFileSize))
					else:
						self._logger.warn(" - %s is not existed in HDFS" %(fileName))
				index += 1
			hdfsFileList = searchFileOfTheSeries(getAllowedMinPeriod(), hdfsFileMap)
			if hdfsFileList:
				self._sendData(startTS, endTS, analysisType, sid, did, countryCode, timezone, hdfsFileList, debugMode, debugMode)

	def _usageSearch(self, params):
		self._logger.info('# 2. Arrange period by each Site ID')
		analysisType, startTS, endTS, siteInfo, _, _, countryCode, timezone, rExtOpt, debugMode = self._getParamElements(params)
		sids = siteInfo.keys()
		sidPeriodMap = self._getArrangedSidPeriodMap(startTS, endTS, sids)
		self._logger.debug(" - target sid : %s" %(str(sids)))

		self._logger.info('# 3. Generate File Path')
		sidHdfsFilePathMap = self._genHdfsFilePath(sidPeriodMap, countryCode, timezone)

		self._logger.info('# 4. Check existed files')
		searchSids = []
		for sid in sidHdfsFilePathMap.keys():
			for fileName in sidHdfsFilePathMap[sid].keys():
				filePath = sidHdfsFilePathMap[sid][fileName]
				hdfsFilePath, hdfsFileSize = self._hdfsClient.existHdfsFiles(filePath, fileName)
				if hdfsFileSize and hdfsFilePath:
					self._logger.debug("[%d] : %s B  %s" %(sid, cvtNumFormat(hdfsFileSize).rjust(11), hdfsFilePath))

				if hdfsFilePath:
					if hdfsFileSize > 0:
						did = siteInfo[sid]
						splitedTS = hdfsFilePath.split('/')[-1].split('.')[0].split('-')
						fromTS = int(splitedTS[0])
						toTS = int(splitedTS[1])
						if analysisType == 'candidate':
							continue
						self._logger.debug(" - send nilm data file path : %s" %(hdfsFilePath))
						self._sendData(fromTS, toTS, analysisType, sid, did, countryCode, timezone, [hdfsFilePath], rExtOpt, debugMode)
					else:
						self._logger.warn(" - zero file path : %s" %hdfsFilePath)	
				else:
					self._logger.warn(" - not found a file path : %d" %sid)
					searchSids.append(sid)

		self._logger.info("# 5. Search site ids : %s" %(str(searchSids)))
		if not searchSids:
			self._sendMessageQ.put_nowait(genReqTajoEnable(self._jobType))
			return

		self._logger.info('# 6. Generate Tajo query strings')
		searchSids = list(set(searchSids))
		queryPathMap = TajoQueryGenerator(self._logger).doGenerateQuery(self._jobType, params)

		self._logger.info('# 7. Execute Query')
		tempFilePathList = TajoQueryExecutor(self._logger, self._sendMessageQ, self._jobType).doExecuteQuery(queryPathMap)
		tempFilePathList = list(set(tempFilePathList))
		for tempFilePath in tempFilePathList:
			self._logger.info('- temp file path : %s' %tempFilePath)

		self._logger.info('# 8. Queried Data ETL')
		TajoDataAggregator(logger=self._logger, sendMessageQ=self._sendMessageQ, jobType=self._jobType, offerType=self._offerType, searchSids=searchSids, params=params, hdfsClient=self._hdfsClient).doUsageAggregation(tempFilePathList)

		for filePath in tempFilePathList:
			os.remove(filePath)

	def _getArrangedSidPeriodMap(self, startTS, endTS, sids):
		periodList = getDailyPeriodSet(startTS, endTS)
		sidPeriodMap = {}
		for sid in sids:
			sidPeriodMap[sid] = periodList
		return sidPeriodMap

	def _genHdfsFilePath(self, sidPeriodMap, countryCode, timezone):
		def getShardingPath(shardingLength, sid):
			shardingKey = sid[len(sid)-shardingLength:len(sid)]
			return shardingKey

		sidHdfsFilePathMap = {}
		for sid in sidPeriodMap.keys():
			for period in sidPeriodMap[sid]:
				startTS = period[0]
				endTS = period[1]
				hdfsNilmRootPath = getHdfsNilmPath()
				shardingKey = getShardingPath(3, str(sid))
				city = timezone.split('/')[1]
				fileName = '%d-%d.%s' %(startTS, endTS, sid)
				filePath = '%s/predict/%s_%s/%s/%d/%s' %(hdfsNilmRootPath, countryCode, city, shardingKey, sid, convertTS2Date(startTS))
				if sidHdfsFilePathMap.has_key(sid):
					sidHdfsFilePathMap[sid][fileName] = filePath
				else:
					sidHdfsFilePathMap[sid] = {fileName : filePath}
		return sidHdfsFilePathMap

	def _sendData(self, startTS, endTS, analysisType, sid, did, countryCode, timezone, hdfsFileList, rExtOpt=None, debugMode=False):
		if analysisType == 'candidate':
			return False
		params = {
			'startTS': startTS,
			'endTS': endTS,
			'analysisType': analysisType,
			'sid': sid,
			'did': did,
			'lfid': 3,
			'country': countryCode,
			'timezone': timezone,
			'filePath': hdfsFileList,
			'rExtOpt': rExtOpt,
			'debugMode': debugMode
		}
		try:
			self._sendCount += 1
			self._logger.debug("[%s] %s" %(str(self._sendCount).rjust(3), str(params['filePath'])))
			result = self._sendMessageQ.put_nowait(genResTajoData(self._jobType, self._offerType, params))
			return result
		except Exception, e:
			self._logger.exception(e)
			return False
