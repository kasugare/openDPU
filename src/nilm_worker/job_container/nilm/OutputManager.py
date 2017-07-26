#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from data_store.DataServiceManager import DataServiceManager
import time

PROCESS_NAME = 'OUTPUT_MANAGER'

class OutputManager:
	def __init__(self, outputQ):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('---------- [ Output Manager ] ----------')
		self._dataManager = DataServiceManager(self._logger)
		self._outputQ = outputQ

	def run(self):
		while True:
			self._logger.info("# Ready to run for nilm usage insert #")
			outputMsg = self._outputQ.get()
			outputList = []
			try:
				if not outputMsg:
					pass
				elif outputMsg['jobType'] == 'USAGE' or outputMsg['jobType'] == 'usage':
					outputList.append(outputMsg)

					for _ in range(self._outputQ.qsize()):
						outputList.append(self._outputQ.get())

					sids = [outputMsg['sid'] for outputMsg in outputList]
					
					self._logger.info(">"*50)
					self._logger.info('# Q size : %d, sids : %s' %(self._outputQ.qsize(), sids))
					self._logger.info("<"*50)

					siteVfidMap = self._dataManager.getNilmSitesVirtualFeederMap(sids)
					qhourlyDataSet = self._cvtNilmQHourlyOutputFormat(siteVfidMap, outputList)
					hourlyDataSet = self._cvtNilmHourlyOutputFormat(siteVfidMap, outputList)
					dailyDataSet = self._cvtNilmDailyOutputFormat(siteVfidMap, outputList)

					self._dataManager.setNilmFeederUsage(qhourlyDataSet, hourlyDataSet, dailyDataSet)
			except ValueError, e:
				self._logger.exception(e)
				for outputMsg in outputList:
					self._logger.error(str(outputMsg))
			except Exception, e:
				self._logger.exception(e)
				for outputMsg in outputList:
					self._outputQ.put_nowait(outputMsg)
				time.sleep(2)
			finally:
				time.sleep(0.2)

	def _getArrangedUsageSet(self, usageTimeType='qhourly', sid=0, dailyBasedTS=0, siteVfidMap={}, usageSet={}, appOnSet={}, appCount={}):
		def getTimeGap(usageTimeType):
			if usageTimeType == 'qhourly':
				timeGap = 900000
			elif usageTimeType == 'hourly':
				timeGap = 3600000
			else:
				timeGap = 0
			return timeGap

		def getAppOnOffStatus(appOnSet, nfid, time=None):
			powerOn = False
			if time:
				if appOnSet[nfid][time] == 1:
					powerOn = True
				else:
					powerOn = False
			else:
				if appOnSet[nfid] == 1:
					powerOn = True
				else:
					powerOn = False
			return powerOn

		def usageFilter(nfid, nfidUsage):
			if int(nfid) == 999 and nfidUsage < 0:
				nfidUsage = 0
			return nfidUsage

		mwhUnit = 1000
		arrangedNilmDataSet = []
		timeGap = getTimeGap(usageTimeType)

		if not siteVfidMap.has_key(sid):
			return arrangedNilmDataSet

		vfidMappingTable = siteVfidMap[sid]

		for nfid in usageSet.keys():
			if not vfidMappingTable.has_key(int(nfid)):
				self._logger.warn('- not exist vfid, sid : %d, nfid : %s' %(sid, nfid))
				continue

			vfid = vfidMappingTable[int(nfid)]

			if usageTimeType == 'qhourly' or usageTimeType == 'hourly':
				for time in usageSet[nfid]:
					if not time.isdigit():
						self._logger.warn('- wrong string %s : %s' %(usageType, time))
						self._logger.warn('- %s usage : %s' %(usageType, str(usageSet)))
						self._logger.warn('- %s appliance on/off : %s' %(usageType, str(appOnSet)))
						continue

					usageTS = dailyBasedTS + (timeGap * int(time))
					nfidUsage = usageFilter(nfid, int(usageSet[nfid][time] * mwhUnit))
					powerOn = getAppOnOffStatus(appOnSet, nfid, time)
					
					arrangedNilmDataSet.append({'date': usageTS, 'unitperiodusage': nfidUsage, 'virtualfeederid': vfid, 'poweron': powerOn})
			elif usageTimeType == 'daily':
				nfidUsage = usageFilter(nfid, int(usageSet[nfid] * mwhUnit))
				powerOn = getAppOnOffStatus(appOnSet, nfid)
				usageCount = appCount[nfid]

				arrangedNilmDataSet.append({'date': dailyBasedTS, 'unitperiodusage': nfidUsage, 'virtualfeederid': vfid, 'poweron': powerOn, 'usagecount': usageCount})
		return arrangedNilmDataSet

	def _cvtNilmQHourlyOutputFormat(self, siteVfidMap, outputList):
		usageTimeType = 'qhourly'
		arrangedNilmDataSet = []

		for outputMsg in outputList:
			sid = outputMsg['sid']
			dailyBasedTS = outputMsg['dbTS']
			qHourlyUsage = outputMsg['qHourlyUsage']
			qHourlyAppOn = outputMsg['qHourlyAppOn']

			if not siteVfidMap.has_key(sid):
				continue
			nilmDataSet = self._getArrangedUsageSet(usageTimeType=usageTimeType, sid=sid, dailyBasedTS=dailyBasedTS, siteVfidMap=siteVfidMap, usageSet=qHourlyUsage, appOnSet=qHourlyAppOn)
			arrangedNilmDataSet.extend(nilmDataSet)

		return arrangedNilmDataSet

	def _cvtNilmHourlyOutputFormat(self, siteVfidMap, outputList):
		usageTimeType = 'hourly'
		arrangedNilmDataSet = []

		for outputMsg in outputList:
			sid = outputMsg['sid']
			dailyBasedTS = outputMsg['dbTS']
			hourlyUsage = outputMsg['hourlyUsage']
			hourlyAppOn = outputMsg['hourlyAppOn']

			if not siteVfidMap.has_key(sid):
				continue

			nilmDataSet = self._getArrangedUsageSet(usageTimeType=usageTimeType, sid=sid, dailyBasedTS=dailyBasedTS, siteVfidMap=siteVfidMap, usageSet=hourlyUsage, appOnSet=hourlyAppOn)
			arrangedNilmDataSet.extend(nilmDataSet)

		return arrangedNilmDataSet

	def _cvtNilmDailyOutputFormat(self, siteVfidMap, outputList):
		usageTimeType = 'daily'
		arrangedNilmDataSet = []

		for outputMsg in outputList:
			sid = outputMsg['sid']
			dailyBasedTS = outputMsg['dbTS']
			dailyUsage = outputMsg['dailyUsage']
			dailyAppOn = outputMsg['dailyAppOn']
			dailyCount = outputMsg['dailyCount']

			if not siteVfidMap.has_key(sid):
				continue

			nilmDataSet = self._getArrangedUsageSet(usageTimeType=usageTimeType, sid=sid, dailyBasedTS=dailyBasedTS, siteVfidMap=siteVfidMap, usageSet=dailyUsage, appOnSet=dailyAppOn, appCount=dailyCount)
			arrangedNilmDataSet.extend(nilmDataSet)

		return arrangedNilmDataSet
