#!/usr/bin/env python
# -*- coding: utf-8 -*-

class ETableQueryPool:
	def __init__(self, logger):
		self._logger = logger

	def getSelectNilmDailyEnabledMapQuery(self, dailyTSs, vifds):
		strVfids = ','.join([str(vfid) for vfid in vifds])
		startTS = dailyTSs[0]
		endTS = dailyTSs[-1]

		tableName = 'nilm_daily_feeder_usage'
		resultColumn = ['virtualfeederid AS vfid', 'blobAsBigint(timestampAsBlob(date)) AS bts', 'poweron', 'unitperiodusage AS upu']
		where = 'virtualfeederid in (%s) and date >= %d and date <= %d' %(strVfids, startTS, endTS)
		return tableName, resultColumn, where
