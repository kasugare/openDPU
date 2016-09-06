#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_datetime import convertTS2Date
from data_store.DataServiceManager import DataServiceManager
from nilm_core.app_daily_load import AppDailyLoadR
import traceback
import sys

NILM_COLUMN_SET = ['timestamp','voltage','current','active_power','reactive_power','apparent_power','power_factor']

class NilmAppUsage:
	def __init__(self, logger, jobId, jobType):
		self._dataManager = DataServiceManager(logger)
		self._logger = logger
		self._jobId = jobId
		self._jobType = jobType

	def doProcess(self, hz, sid, did, lfid, feederDataFilePath, dailyBasedTS = None):
		self._logger.info("# [%s] Do nilm usages, sid : %d, did : %d, lfid : %d, based date : %s, freq : %d hz" %(self._jobId, sid, did, lfid, convertTS2Date(dailyBasedTS), hz))
		self._logger.info(" - file path : %s" %feederDataFilePath)
		appUsage = AppDailyLoadR(self._logger)
		resultMap = {}

		try:
			metaInfo = self._dataManager.getNilmMetaInfo(sid, did, lfid)
			appUsage.set_data(feederDataFilePath, hz)
			appUsage.set_app_info(metaInfo)
			appUsage.do_compute()
			appUsageMap = appUsage.get_usage()

			if not appUsageMap:
				self._logger.warn('- [%s] empty appUsageMap! sid : %d, file path : %s' %(self._jobId, sid, feederDataFilePath))
				return None

			if appUsageMap:
				hourlyUsage, dailyUsage = self._cvtUsageData(appUsageMap)
				hourlyAppOn = self._checkHourlyAppOnOff(hourlyUsage)
				dailyAppOn = self._checkDailyAppOnOff(dailyUsage)

				resultMap = {
					'dbTS': dailyBasedTS,
					'jobType': self._jobType,
					'sid': sid,
					'did': did,
					'lfid': lfid,
					'hourlyUsage': hourlyUsage,
					'dailyUsage': dailyUsage,
					'hourlyAppOn': hourlyAppOn,
					'dailyAppOn': dailyAppOn
				}
		except Exception, e:
			self._logger.exception(e)
			self._logger.error(appUsageMap)
		finally:
			appUsage.pid.terminate()
		return resultMap

	def _cvtUsageData(self, appUsageMap):
		hourlyUsage = {}
		dailyUsage = {}

		for nfid in appUsageMap.keys():
			hourlyUsage[nfid] = appUsageMap[nfid]['hourly']
			dailyUsage[nfid] = appUsageMap[nfid]['daily']
		return hourlyUsage, dailyUsage

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
