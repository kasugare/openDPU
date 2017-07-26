#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_datetime import htime, convertTS2DailyTimestampList
from data_store.DataServiceManager import DataServiceManager
import copy

class NilmOutputManager:
	def __init__(self, logger):
		self._logger = logger

	def doProcess(self, startTS, endTS, sids):
		checkedResultSet = {}

		dailyTSs = convertTS2DailyTimestampList(startTS, endTS)
		dbServiceManager = DataServiceManager(self._logger)

		self._logger.info("# Get sid with vfid map")
		vfidMap = dbServiceManager.getNilmVfidsMap(sids)
		vfids = vfidMap.keys()
		sids = vfidMap.values()
		sids = list(set(sids))

		shardingMap = self._getShardingTargetInfo(vfids, dailyTSs)
		for (targetVfids, dailyTSs) in shardingMap:
			self._logger.info("# sharding map :: vfids : %d,  dailyTs : %d" %(len(targetVfids), len(dailyTSs)))

			self._logger.info("# Get virtual feeder's usages")
			vfidsUsageMap = dbServiceManager.getNilmDailyEnabledMap(dailyTSs, targetVfids)
			# self._printDataSet(vfidsUsageMap)

			self._logger.info("# Check output result set")
			checkedResultSet = self._checkOutputResult(dailyTSs, vfidMap, vfidsUsageMap, checkedResultSet)

		checkedResultSet = self._cvtBtsToGmt9Ts(checkedResultSet)
		# self._printResultSet(checkedResultSet)
		return checkedResultSet

	def _getShardingTargetInfo(self, vfids, dailyTSs):
		limitSearchSize = 5000
		shardingMap = []

		sizeOfVfids = len(vfids)
		sizeOfDTSs = len(dailyTSs)

		if sizeOfDTSs <= limitSearchSize:
			if sizeOfVfids * sizeOfDTSs > limitSearchSize:
				targetVfids = copy.deepcopy(vfids)
				startIndex = 0
				endIndex = 0
				currentSetSize = 0

				while True:
					maxSearchSize = limitSearchSize / sizeOfDTSs
					if currentSetSize + maxSearchSize > sizeOfVfids:
						endIndex += sizeOfVfids - currentSetSize
					else:
						endIndex += maxSearchSize
					targetVfids = vfids[startIndex:endIndex]
					shardingMap.append((targetVfids, dailyTSs))
					startIndex = endIndex
					currentSetSize += len(targetVfids)
					if currentSetSize >= sizeOfVfids:
						break
			else:
				shardingMap = [(vfids, dailyTSs)]
		return shardingMap
		
	def _checkOutputResult(self, dailyTSs, vfidMap, vfidsUsageMap, checkedResultSet):
		def checkOutputByBts(successfulVfidSet):
			resultSet = {}
			successfulBTSs = successfulVfidSet.keys()
			for bts in dailyTSs:
				if bts in successfulBTSs:
					resultSet[bts] = successfulVfidSet[bts]
					resultSet[bts]['success'] = True
				else:
					resultSet[bts] = {'success': False}
			return resultSet

		def mergeResultSet(prevResultSet, currentResultSet):
			for bts in prevResultSet.keys():
				prevSuccess = prevResultSet[bts]['success']
				if currentResultSet.has_key(bts):
					currSuccess = currentResultSet[bts]['success']
					if prevSuccess and not currSuccess:
						currentResultSet[bts] = prevResultSet[bts]
				else:
					currentResultSet[bts] = prevResultSet[bts]
			return currentResultSet

		activatedVfids = vfidsUsageMap.keys()
		for vfid in activatedVfids:
			sid = vfidMap[vfid]
			successfulVfidSet = vfidsUsageMap[vfid]
			resultSet = checkOutputByBts(successfulVfidSet)
			if checkedResultSet.has_key(sid):
				prevResultSet = checkedResultSet[sid]
				resultSet = mergeResultSet(prevResultSet, resultSet)
			checkedResultSet[sid] = resultSet
		vfids = vfidMap.keys()

		inactivatedVfids = list(set(vfids).difference(activatedVfids))
		for vfid in inactivatedVfids:
			sid = vfidMap[vfid]
			successfulVfidSet = {}
			resultSet = checkOutputByBts(successfulVfidSet)
			if checkedResultSet.has_key(sid):
				prevResultSet = checkedResultSet[sid]
				resultSet = mergeResultSet(prevResultSet, resultSet)
			checkedResultSet[sid] = resultSet

		return checkedResultSet

	def _cvtBtsToGmt9Ts(self, checkedResultSet):
		resultSet = {}
		for sid in checkedResultSet.keys():
			for bts in checkedResultSet[sid].keys():
				gmt9Bts = bts + 32400000
				if resultSet.has_key(sid):
					resultSet[sid][gmt9Bts] = checkedResultSet[sid][bts]
					del checkedResultSet[sid][bts]
				else:
					resultSet[sid] = {gmt9Bts: checkedResultSet[sid][bts]}
					del checkedResultSet[sid][bts]
		return resultSet

	def _printDataSet(self, vfidsUsageMap):
		for vfid in vfidsUsageMap.keys():
			self._logger.debug(" # %d" %vfid)
			bTSs = vfidsUsageMap[vfid].keys()
			bTSs.sort()
			for bts in bTSs:
				upu = str(vfidsUsageMap[vfid][bts]['upu'])
				powerOn = str(vfidsUsageMap[vfid][bts]['poweron'])
				self._logger.debug("  - %s (%d)" %(htime(bts), bts))

	def _printResultSet(self, checkedResultSet):
		for sid in checkedResultSet.keys():
			self._logger.debug(" # %d" %sid)
			bTSs = checkedResultSet[sid].keys()
			bTSs.sort()
			for bts in bTSs:
				isSuccess = checkedResultSet[sid][bts]['success']
				if isSuccess:
					self._logger.debug("   - %s (%d) : sucsess : %s" %(htime(bts), bts, str(isSuccess)))
				else:
					self._logger.warn(" - %s (%d) : sucsess : %s" %(htime(bts), bts, str(isSuccess)))

