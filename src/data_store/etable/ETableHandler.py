#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_datastore import getDbExecutableSize, getETableExecutorInfo
from common.util_datetime import convertUnixTimeToUtcTimestamp
from ecommon.etable.ETable import ETable
from ecommon.etable.ETableRowsOperator import ETableRowsOperator
from ecommon.etable.ETableCassandraExecutor import ETableCassandraExecutor
from ETableQueryPool import ETableQueryPool
import traceback
import time

class ETableHandler:
	def __init__(self, logger):
		self._logger = logger
		self._queryPool = ETableQueryPool(logger)

	def _executeWriteQuery(self, usageData, tableName, executionSize=1000):
		if not usageData:
			return

		confingInfo =  getETableExecutorInfo()
		executor = ETableCassandraExecutor(**confingInfo)

		startIndex = 0
		remainSize = len(usageData)
		dataSize = len(usageData)
		splitedDataSet = None

		try:
			self._logger.info(" - usage data size : %d" %(dataSize))
			feederUsageTable = ETable(tableName, [], executor)
			while remainSize > 0:
				if executionSize >= dataSize:
					executionSize = dataSize

				startIndex = dataSize - remainSize
				splitedDataSet = usageData[startIndex:startIndex+executionSize]
				remainSize -= len(splitedDataSet)
				feederUsageTable.insert(usageData)
				self._logger.info("\tend of Size : %d / %d" %(dataSize - remainSize, dataSize))
		except Exception, e:
			errorString = str(e)
			self._logger.error('# unable to make in from wrong data : %s' %errorString)

			splitedErrorString = errorString.replace("'", "").replace('"','').replace('=', ' ').split(' ')
			if len(splitedErrorString) > 2 and splitedErrorString[0] == 'code' and splitedErrorString[1] == '1100':
				self._logger.warn("# Cassandra timeout. It going to sleep around 3sec and then will retry.")
				time.sleep(3)
				splitedDataSet.extend(usageData[start+executionSize:])
				self._executeWriteQuery(splitedDataSet, tableName)
			elif len(splitedErrorString) > 2 and splitedErrorString[0] == 'code' and splitedErrorString[1] == '2200':
				self._logger.warn("# Detected wrong data. It going to remove wrong data and then will retry.")
			else:
				errorExecutionSize = int(executionSize/2)
				if errorExecutionSize > 0:
					errorDataListSet = []
					errorDataListSet.append(splitedDataSet[:errorExecutionSize])
					errorDataListSet.append(splitedDataSet[errorExecutionSize:])
					for errorDataSet in errorDataListSet:
						self._executeWriteQuery(errorDataSet, tableName, executionSize=int(executionSize/2))

	def _executeReadQuery(self, tableName, resultColumn, where):
		self._logger.info("[CASSANDRA] select query execution : %s" %(where))
		self._logger.debug(" - table name : %s" %(tableName))
		self._logger.debug(" - select columns : %s" %(str(resultColumn)))
		self._logger.debug(" - where : %s" %(where))

		confingInfo = getETableExecutorInfo()
		executor = ETableCassandraExecutor(**confingInfo)
		etable = ETable(tableName, [], executor)
		items = etable.select(resultColumn, where, 0)
		return items

	def insertNilmQHourlyFeederUsage(self, arrangedUsageDataSet):
		self._logger.info("# Insert qhourly feeder usage data to the cassandra.")
		for usageSet in arrangedUsageDataSet:
			self._logger.debug(str(usageSet))
		self._executeWriteQuery(arrangedUsageDataSet, 'nilm_quarterhourly_feeder_usage')

	def insertNilmHourlyFeederUsage(self, arrangedUsageDataSet):
		self._logger.info("# Insert hourly feeder usage data to the cassandra.")
		for usageSet in arrangedUsageDataSet:
			self._logger.debug(str(usageSet))
		self._executeWriteQuery(arrangedUsageDataSet, 'nilm_hourly_feeder_usage')

	def insertNilmDailyFeederUsage(self, arrangedUsageDataSet):
		self._logger.info("# Insert daily feeder usage data to the cassandra.")
		for usageSet in arrangedUsageDataSet:
			self._logger.debug(str(usageSet))
		self._executeWriteQuery(arrangedUsageDataSet, 'nilm_daily_feeder_usage')

	def _genArrangedUsageDataSet(self, datetime, nfidUsage, vfid):
		arrangedUsageDataSet = {
			'date': convertUnixTimeToUtcTimestamp(datetime),
			'unitperiodusage': nfidUsage,
			'virtualfeederid': vfid
		}
		return [arrangedUsageDataSet]

	def selectNilmDailyEnabledMap(self, dailyTSs, vfids):
		self._logger.info("# Select daily virtual feeder enabled set on the cassandra.")
		tableName, resultColumn, where = self._queryPool.getSelectNilmDailyEnabledMapQuery(dailyTSs, vfids)
		items = self._executeReadQuery(tableName, resultColumn, where)

		outputMap = {}
		for item in items:
			vfid = item['vfid']
			bts = item['bts']
			if outputMap.has_key(vfid):
				outputMap[vfid][bts] = {'poweron': item['poweron'], 'upu': item['upu']}
			else:
				outputMap[vfid] = {bts: {'poweron': item['poweron'], 'upu': item['upu']}}
		return outputMap
