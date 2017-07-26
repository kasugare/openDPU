#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_collector import getAllowedProtocol, getTajoTmpPath
from nilm_worker.job_container.tajo_gateway.tajo_collector.QueryGenerator import QueryGenerator
import time

class TajoQueryGenerator(QueryGenerator):
	def __init__(self, logger):
		self._logger = logger
		self._deviceType = getAllowedProtocol()

	def doGenerateQuery(self, jobType, params):
		analysisType, startTS, endTS, siteInfo, deviceCh, nilmFreq, _, _ = self._getParamElements(params)
		sids = siteInfo.keys()
		queryPathMap = {}

		if jobType == 'NILM_RAW_DATA':
			tempPath = '%s/%d_%d_%d_%d' %(getTajoTmpPath(), nilmFreq, startTS, endTS, int(time.time()))
			queryPathMap = self._genQueryList(startTS, endTS, sids, deviceCh, nilmFreq, tempPath)

		for tempPath in queryPathMap.keys():
			self._logger.debug('- %s : %s' %(tempPath, queryPathMap[tempPath]))
		return queryPathMap

	def _getParamElements(self, params):
		analysisType = params['analysisType']
		startTS = params['startTS']
		endTS = params['endTS']
		siteInfo = params['target']
		deviceCh = params['deviceCh']
		nilmFreq = params['nilmFreq']
		countryCode = params['country']
		timezone = params['timezone']
		return analysisType, startTS, endTS, siteInfo, deviceCh, nilmFreq, countryCode, timezone

	def _genQueryList(self, startTS, endTS, sids, deviceCh, nilmFreq, tempPath):
		queryList = []
		startConversionTS = 1471878000000
		endConversionTS = 1472482800000

		partitions = self.genQueryPartitions(startTS, endTS)
		if startTS >= startConversionTS and startTS <= endConversionTS or endTS >= startConversionTS and endTS < endConversionTS:
			def genColumnString(deviceCh, nilmFreq):
				extractQueryElements = ['A.sid', 'A.did', 'A.dts']
				extractLayer1Elements = ['A.vrmsa', 'A.irms']
				extractLayer2Elements = ['watt', 'var']
				chIndexList = []

				if deviceCh == 1:
					chIndexList.append(0)
				elif deviceCh > 1:
					for index in range(deviceCh):
						chIndexList.append(index+1)

				for chIndex in chIndexList:
					for extractLayer1Column in extractLayer1Elements:
						layer1ColumnString = '%s%d' %(extractLayer1Column, chIndex)
						extractQueryElements.append(layer1ColumnString)

					for extractLayer2Column in extractLayer2Elements:
						for freqIndex in range(nilmFreq):
							layer2ColumnString = 'A.%s%d_%d' %(extractLayer2Column, chIndex, freqIndex)
							extractQueryElements.append(layer2ColumnString)
				columString = ', '.join(extractQueryElements)
				return columString

			columnString = genColumnString(deviceCh, nilmFreq)
			legacyQueryString = self._getEdm3QueryString('edm3_201', startTS, endTS, partitions, sids, deviceCh, nilmFreq)
			
			newQueryString = self._getEdm3QueryString('edm3_300', startTS, endTS, partitions, sids, deviceCh, nilmFreq)

			unionQueryString = """SELECT %s FROM (%s UNION %s) A ORDER BY A.sid, A.dts ASC""" %(columnString, legacyQueryString[0], newQueryString[0])
			queryList.append(unionQueryString)
		elif endTS <= startConversionTS:
			tableName = 'edm3_201'
			queryList += self._getEdm3QueryString(tableName, startTS, endTS, partitions, sids, deviceCh, nilmFreq)
		elif startTS >= endConversionTS:
			tableName = 'edm3_300'
			queryList += self._getEdm3QueryString(tableName, startTS, endTS, partitions, sids, deviceCh, nilmFreq)

		queryPathMap = {}
		for queryString in queryList:
			if tempPath:
				if queryPathMap.has_key(tempPath):
					queryPathMap[tempPath].append(queryString)
				else:
					queryPathMap[tempPath] = [queryString]
		return queryPathMap

	def _getEdm3QueryString(self, tableName, startTS, endTS, partitions, sids, deviceCh, nilmFreq):
		def genColumnString(deviceCh, nilmFreq):
			extractQueryElements = ['sid', 'did', 'dts']
			extractLayer1Elements = ['vrmsa', 'irms']
			extractLayer2Elements = ['watt', 'var']
			chIndexList = []

			if deviceCh == 1:
				chIndexList.append(0)
			elif deviceCh > 1:
				for index in range(deviceCh):
					chIndexList.append(index+1)

			for chIndex in chIndexList:
				for extractLayer1Column in extractLayer1Elements:
					layer1ColumnString = '%s%d' %(extractLayer1Column, chIndex)
					extractQueryElements.append(layer1ColumnString)

				for extractLayer2Column in extractLayer2Elements:
					for freqIndex in range(nilmFreq):
						layer2ColumnString = '%s%d_%d/1000. AS %s%d_%d' %(extractLayer2Column, chIndex, freqIndex, extractLayer2Column, chIndex, freqIndex)
						extractQueryElements.append(layer2ColumnString)
			columString = ', '.join(extractQueryElements)
			return columString

		queryList = []
		csvYear, csvMonth, csvDay = self.cvtPartitionElement(partitions)
		columString = genColumnString(deviceCh, nilmFreq)
		queryString = """SELECT %s
			 FROM %s 
			 WHERE dts >= %d
				 AND dts < %d
				 AND csv_year in (%s)
				 AND csv_month in (%s)
				 AND csv_day in (%s)""" %(columString, tableName, startTS, endTS, csvYear, csvMonth, csvDay)
		queryList.append(queryString)
		queryList = self.addParams(queryList, sids)
		queryList = self.addOrderbySidDts(queryList)
		queryList = self.stripQueryString(queryList)
		return queryList
