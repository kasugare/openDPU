#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_hdfs import getHdfsNilmPath
from common.util_datetime import convertOneDayBaseTimestampGmt9, convertTS2Date, getDailyPeriodicTS, getDailyPeriodSet
from protocol.message_pool.MessageGenerator import genResTajoData
import time

class TajoDataAggregator:
	def __init__(self, logger, sendMessageQ, jobType, offerType, searchSids, params, hdfsClient):
		self._logger = logger
		self._sendMessageQ = sendMessageQ
		self._jobType = jobType
		self._offerType = offerType
		self._searchSids = searchSids
		self._hdfsClient = hdfsClient
		self._analysisType, self._startTS, self._endTS, self._siteInfo, _, _, self._countryCode, self._timezone, self._rExtOpt, self._debugMode = self._getParamElements(params)
		self._sids = self._siteInfo.keys()

	def doUsageAggregation(self, tempFilePathList):
		def getSidByHdfsFilePath(hdfsFilePath):
			try:
				return int(hdfsFilePath.split('.')[1].split('_')[0])
			except:
				return

		hdfsFilePath = None
		regSids = []
		startTS = self._startTS
		endTS = self._endTS
		analysisType = self._analysisType

		for tempFilePath in tempFilePathList:
			fd = open(tempFilePath, 'r')
			hdfsFilePath = None
			usageRawDataList = []

			latestSid = None
			latestBDTS = None
			fromTS = None
			toTS = None
			sid_did = None

			while True:
				rawData = fd.readline()
				if not rawData:
					if latestBDTS:
						self._logger.debug("# step 1 : %d" %latestSid)
						hdfsFilePath = self._saveNilmRawDataToHdfs(hdfsFilePath, usageRawDataList)
						self._sendData(fromTS, toTS, analysisType, hdfsFilePath)
						regSids.append(getSidByHdfsFilePath(hdfsFilePath))
						usageRawDataList = []
					fd.close()
					break
				try:
					splitedRawData = rawData.replace(' ','').split(',')

					sid = int(splitedRawData[0])
					did = int(splitedRawData[1])
					dts = int(splitedRawData[2])
					sid_did = '%d_%d' %(sid, did)
					dbts = convertOneDayBaseTimestampGmt9(dts)

					nilmData = self._cvtRawData(splitedRawData)

					if latestSid == None:
						fromTS, toTS = getDailyPeriodicTS(dts)
						hdfsFilePath = self._genHdfsFilePath(fromTS, toTS, 'usage', sid_did)

						latestBDTS = dbts
						latestSid = sid

					if latestSid == sid:
						# UsageSearch
						if not latestBDTS:
							usageRawDataList.append(nilmData)
							latestBDTS = dbts
						elif latestBDTS == dbts:
							usageRawDataList.append(nilmData)
						else:
							self._logger.debug("# step 2 : %d" %(sid))
							hdfsFilePath = self._saveNilmRawDataToHdfs(hdfsFilePath, usageRawDataList)
							if sid in self._searchSids:
								self._sendData(fromTS, toTS, analysisType, hdfsFilePath)
								regSids.append(getSidByHdfsFilePath(hdfsFilePath))
							latestBDTS = dbts
							fromTS, toTS = getDailyPeriodicTS(dts)
							hdfsFilePath = self._genHdfsFilePath(fromTS, toTS, 'usage', sid_did)
							usageRawDataList = [nilmData]
					else:
						# Usage-Rawdata Reset
						self._logger.debug("# step 3 : %d" %(sid))
						hdfsFilePath = self._saveNilmRawDataToHdfs(hdfsFilePath, usageRawDataList)
						if sid in self._searchSids:
							self._sendData(fromTS, toTS, analysisType, hdfsFilePath)
							regSids.append(getSidByHdfsFilePath(hdfsFilePath))
						latestBDTS = dbts
						fromTS, toTS = getDailyPeriodicTS(dts)
						hdfsFilePath = self._genHdfsFilePath(fromTS, toTS, 'usage', sid_did)
						usageRawDataList = [nilmData]
						latestSid = sid
				except Exception, e:
					self._logger.warn('- wrong raw data : %s' %rawData)
					self._logger.exception(e)

		regSids = list(set(regSids))
		notRegSids = list(set(self._searchSids).difference(set(regSids)))
		self._logger.debug('# search sids : %s' %(str(self._searchSids)))
		self._logger.debug('# registrated sids : %s' %(str(regSids)))
		self._logger.debug('# not registrated sids : %s' %(str(notRegSids)))
		
		if notRegSids:
			for sid in notRegSids:
				did = self._siteInfo[sid]
				sid_did = '%d_%d' %(sid, did)
				for dailySet in getDailyPeriodSet(self._startTS, self._endTS):
					fromTS = dailySet[0]
					toTS = dailySet[1]
					hdfsFilePath = self._genHdfsFilePath(fromTS, toTS, 'usage', sid_did)
					self._logger.debug("# step 4")
					self._saveNilmRawDataToHdfs(hdfsFilePath, [])
		self._logger.debug('# Tajo ETL Done')

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

	def _sendData(self, startTS, endTS, analysisType, filePath):
		sid_did = '_'.join(filePath.split('.')[1].split('_')[:2])
		try:
			if analysisType == 'candidate':
				return False
			params = self._genResult(startTS, endTS, analysisType, sid_did, [filePath])
			return self._sendMessageQ.put_nowait(genResTajoData(self._jobType, self._offerType, params))
		except Exception, e:
			self._logger.exception(e)
			return False

	def _cvtRawData(self, splitedRawData):
		dts = splitedRawData[2]
		data = [str(round(int(dts)/1000., 2))]
		for value in splitedRawData[3:]:
			try:
				data.append(str(round(float(value), 2)))
			except:
				pass
		data = ",".join(data)
		return data

	def _genHdfsFilePath(self, startTS, endTS, analysisType, sid_did):
		shardingLength = 3
		sid = sid_did.split('_')[0]
		hdfsNilmRootPath = getHdfsNilmPath()
		shardingKey = sid[len(sid)-shardingLength:len(sid)]
		fileName = '%d-%d.%s_3.csv' %(startTS, endTS, sid_did)
		city = str(self._timezone.split('/')[1])

		filePath = '%s/predict/%s_%s/%s/%s/%s/%s' %(hdfsNilmRootPath, self._countryCode, city, shardingKey, sid, convertTS2Date(startTS), fileName)
		return filePath

	def _saveNilmRawDataToHdfs(self, hdfsFilePath, nilmRawDataList):
		self._logger.info('# Save nilm raw data in HDFS : %s, data length : %s, reception rate : %0.2f %%' %(hdfsFilePath, len(nilmRawDataList), len(nilmRawDataList)/86400.*100))
		try:
			self._hdfsClient.write(nilmRawDataList, hdfsFilePath, linefeed=True)
		except Exception, e:
			self._logger.exception(e)
			return None
		return hdfsFilePath

	def _genResult(self, startTS, endTS, analysisType, sid_did, nilmRawDataPath):
		params = {
			'startTS': startTS,
			'endTS': endTS,
			'analysisType': analysisType,
			'sid': int(sid_did.split('_')[0]),
			'did': int(sid_did.split('_')[1]),
			'lfid': 3,
			'country': self._countryCode,
			'timezone': self._timezone,
			'filePath': nilmRawDataPath,
			'rExtOpt': self._rExtOpt,
			'debugMode': debugMode
		}
		return params
