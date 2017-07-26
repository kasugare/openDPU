#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_datetime import convertTS2Date
from data_store.DataServiceManager import DataServiceManager
from NilmCoreProcess import NilmCoreProcess
import os

class NilmUsagePredictor(NilmCoreProcess):
	def __init__(self, logger, rLogger, jobId, jobType, countryCode, timezone, fsHandler, debugMode):
		NilmCoreProcess.__init__(self, logger, rLogger, debugMode)
		self._dataManager = DataServiceManager(logger)
		self._logger = logger
		self._jobId = jobId
		self._jobType = jobType
		self._countryCode = countryCode
		self._timezone = timezone
		self._fsHandler = fsHandler

	def doProcess(self, sid, did, lfid, feederDataFilePath, dailyBasedTS=None, rExtOpt=None):
		self._logger.info("# [%s] Do nilm usages, sid : %d, did : %d, lfid : %d, based date : %s, country code : %s, timezone : %s" %(self._jobId, sid, did, lfid, convertTS2Date(dailyBasedTS), self._countryCode, self._timezone))
		self._logger.info(" - file path : %s" %feederDataFilePath)
		resultMap = {}
		metaFilePath = None
		countryInfoFilePath = None
		metaInfo = self._dataManager.getNilmMetaInfo(sid, did, lfid)
		
		if not metaInfo:
			self._logger.warn('# Not existed meta sid : %d' %(sid))
			return

		try:
			metaFilePath = self._fsHandler.saveNilmMetaInfoToFile(feederDataFilePath, metaInfo)
			siteInfoFilePath = self._fsHandler.saveSiteCountryInfoToFile(sid, self._countryCode, self._timezone)

			self.setSidInfo(sid, feederDataFilePath)
			appUsageMap = self.runUsagePrediction(metaFilePath, siteInfoFilePath, feederDataFilePath, rExtOpt)

			if appUsageMap:
				qHourlyUsage, hourlyUsage, dailyUsage = self._cvtUsageData(appUsageMap)
				qHourlyAppOn, hourlyAppOn, dailyAppOn = self._cvtPowerOnOff(appUsageMap)
				dailyCount = self._checkDailyCount(appUsageMap)

				resultMap = {
					'dbTS': dailyBasedTS,
					'jobType': self._jobType,
					'sid': sid,
					'did': did,
					'lfid': lfid,
					'qHourlyUsage': qHourlyUsage,
					'hourlyUsage': hourlyUsage,
					'dailyUsage': dailyUsage,
					'qHourlyAppOn': qHourlyAppOn,
					'hourlyAppOn': hourlyAppOn,
					'dailyAppOn': dailyAppOn,
					'dailyCount': dailyCount
				}
			else:
				self._logger.warn('- [%s] empty appUsageMap! sid : %d, file path : %s' %(self._jobId, sid, feederDataFilePath))
		except Exception, e:
			self._logger.exception(e)
		self._logger.info('# remove meta and site info files')
		if metaFilePath:
			os.remove(metaFilePath)
		if siteInfoFilePath:
			os.remove(siteInfoFilePath)
		self._logger.info('# completed daily nilm prediction')
		return resultMap

	def _cvtUsageData(self, appUsageMap):
		qHourlyUsage = {}
		hourlyUsage = {}
		dailyUsage = {}
		for nfid in appUsageMap.keys():
			qHourlyUsage[nfid] = appUsageMap[nfid]['qhourly']
			hourlyUsage[nfid] = appUsageMap[nfid]['hourly']
			dailyUsage[nfid] = appUsageMap[nfid]['daily']
		return qHourlyUsage, hourlyUsage, dailyUsage

	def _cvtPowerOnOff(self, appUsageMap):
		qHourlyAppOn = {}
		hourlyAppOn = {}
		dailyAppOn = {}
		for nfid in appUsageMap.keys():
			qHourlyAppOn[nfid] = appUsageMap[nfid]['qhourly_OnOff']
			hourlyAppOn[nfid] = appUsageMap[nfid]['hourly_OnOff']
			dailyAppOn[nfid] = appUsageMap[nfid]['daily_OnOff']
		return qHourlyAppOn, hourlyAppOn, dailyAppOn

	def _checkHourlyAppOnOff(self, hourlyUsage):
		if not hourlyUsage:
			return None
		checkAppEnabled = lambda x : True if x >= 1 else False
		hourlyAppOn = {}
		for lfid in hourlyUsage.keys():
			for hour in hourlyUsage[lfid].keys():
				usage = hourlyUsage[lfid][hour]
				if hourlyAppOn.has_key(lfid):
					hourlyAppOn[lfid][hour] = checkAppEnabled(usage)
				else:
					hourlyAppOn[lfid] = {hour: checkAppEnabled(usage)}
		return hourlyAppOn

	def _checkDailyAppOnOff(self, dailyUsage):
		if not dailyUsage:
			return None
		checkAppEnabled = lambda x : True if x >= 5 else False
		dailyAppOn = {}
		for lfid in dailyUsage.keys():
			usage = dailyUsage[lfid]
			dailyAppOn[lfid] = checkAppEnabled(usage)
		return dailyAppOn

	def _checkDailyCount(self, appUsageMap):
		dailyCount = {}
		for nfid in appUsageMap.keys():
			if appUsageMap[nfid].has_key('daily_count'):
				dailyCount[nfid] = appUsageMap[nfid]['daily_count']
			else:
				dailyCount[nfid] = -1
		return dailyCount
