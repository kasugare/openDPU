#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_system import getNilmMetaUpdateDailyPeriod, getMaxLearningJob
from common.util_datetime import cvtNextDayOfTS, convertOneDayBaseTimestamp, cvtDayOfTS, htime
from data_store.DataServiceManager import DataServiceManager

class SchedulingHandler:
	def __init__(self, logger):
		self._logger = logger
		self._dataServiceManager = DataServiceManager(logger)

	def doProcess(self):
		self._logger.info("# learner scheduler for load balance")
		schedLearnerMap = self._initSchedLearnerMap()
		self._logger.info("# 1. get target site info")
		hasNoMetaTargets, hasMetaTargets = self._getNilmUpdateTargetInfo()
		totalTargetSize = len(hasNoMetaTargets) + len(hasMetaTargets)

		self._logger.info("# 2. scheduling meta learner")
		schedLearnerMap = self._setLearningPriority(schedLearnerMap, hasNoMetaTargets)
		self._printScheduledTS(schedLearnerMap)

		self._logger.info("# 3. scheduling meta update learner")
		schedLearnerMap = self._setUpdatePriority(schedLearnerMap, hasMetaTargets, totalTargetSize=totalTargetSize)
		self._printScheduledTS(schedLearnerMap)

		self._logger.info("# 4. update scheduled date for meta learner on RDB")
		self._dataServiceManager.setScheduledLearingDateForLoadBalance(schedLearnerMap)

	def _initSchedLearnerMap(self):
		schedLearnerMap = {}
		updatePeriod = getNilmMetaUpdateDailyPeriod()
		schedTSs = [cvtNextDayOfTS(nextDay) for nextDay in range(updatePeriod)]
		schedTSs.sort()

		for schedTS in schedTSs:
			schedLearnerMap[schedTS] = []
		return schedLearnerMap


	def _getNilmUpdateTargetInfo(self):
		nilmLearnerTargetInfo = self._dataServiceManager.getNilmMetaUpdateTargetInfoForLoadBalance()
		hasMetaTargets = []
		hasNoMetaTargets = []
		for hasMeta in nilmLearnerTargetInfo.keys():
			if hasMeta:
				hasMetaTargets = nilmLearnerTargetInfo[hasMeta]
			else:
				hasNoMetaTargets = nilmLearnerTargetInfo[hasMeta]
		hasNoMetaTargets.sort()
		hasMetaTargets.sort()
		return hasNoMetaTargets, hasMetaTargets

	def _setLearningPriority(self, schedLearnerMap, hasNoMetaTargets, totalTargetSize=getMaxLearningJob()):
		updatePeriod = getNilmMetaUpdateDailyPeriod()
		schedTSs = schedLearnerMap.keys()
		schedTSs.sort()

		index = 0
		for sidUpdateTS in hasNoMetaTargets:
			splitedInfo = sidUpdateTS.split('_')
			updateTS = int(splitedInfo[0])
			beUpdatedTS = updateTS + cvtDayOfTS(updatePeriod)
			updateBTS = convertOneDayBaseTimestamp(beUpdatedTS)
			sid = int(splitedInfo[1])
			sizeOfSchedTSs = len(schedTSs)

			while index < sizeOfSchedTSs:
				schedTS = schedTSs[index]
				if len(schedLearnerMap[schedTS]) < totalTargetSize:
					if updateBTS == schedTS:
						schedLearnerMap[schedTS].append(sid)
						break
					elif updateBTS in schedTSs:
						index = schedTSs.index(updateBTS)
						schedTS = schedTSs[index]
						schedLearnerMap[schedTS].append(sid)
						break
					elif updateBTS < schedTS:
						schedLearnerMap[schedTS].append(sid)
						break
					else:
						index += 1
				else:
					index += 1
		return schedLearnerMap

	def _setUpdatePriority(self, schedLearnerMap, hasMetaTargets, totalTargetSize=getMaxLearningJob()):
		updatePeriod = getNilmMetaUpdateDailyPeriod()

		if round(float(totalTargetSize/updatePeriod)) + 1 > totalTargetSize:
			maxSizeOfLearingJob = getMaxLearningJob()
		else:
			maxSizeOfLearingJob = int(round(float(totalTargetSize/updatePeriod)) + 1)

		index = 0
		reverse = False
		schedTSs = schedLearnerMap.keys()
		schedTSs.sort(reverse=reverse)
		hasMetaTargets.sort(reverse=reverse)

		for sidUpdateTS in hasMetaTargets:
			splitedInfo = sidUpdateTS.split('_')
			updateTS = int(splitedInfo[0])
			beUpdatedTS = updateTS + cvtDayOfTS(updatePeriod)
			updateBTS = convertOneDayBaseTimestamp(beUpdatedTS)
			sid = int(splitedInfo[1])
			sizeOfSchedTSs = len(schedTSs)

			while index < sizeOfSchedTSs:
				schedTS = schedTSs[index]
				if len(schedLearnerMap[schedTS]) < maxSizeOfLearingJob:
					if updateBTS == schedTS:
						schedLearnerMap[schedTS].append(sid)
						break
					elif updateBTS <= schedTS:
						schedLearnerMap[schedTS].append(sid)
						break
					else:
						index += 1
				else:
					index += 1
		return schedLearnerMap
			
	def _printScheduledTS(self, schedLearnerMap):
		schedTSs = schedLearnerMap.keys()
		schedTSs.sort()
		for schedTS in schedTSs:
			self._logger.debug('- scheduled date : %s (%d), size of target sids : %d' %(htime(schedTS)[:10], schedTS, len(schedLearnerMap[schedTS])))
		totalSchedSids = 0
		for schedTS in schedTSs:
			totalSchedSids += len(schedLearnerMap[schedTS])
		self._logger.debug("- total scheduled site ids : %d" %(totalSchedSids))
