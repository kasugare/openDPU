#!/usr/bin/env python
# -*- coding: utf-8 -*-

from TajoQueryHelper import TajoQueryHelper

class TajoQueryPool(TajoQueryHelper):
	def __init__(self, logger):
		self._logger = logger

	def _printLog(self, queryList):
		for queryString in queryList:
			queryString = " ".join(queryString.split())
			self._logger.debug(" - %s" %queryString)

	# select
	def getSelectActiveDeviceSiteMap(self, startTS, endTS, tableName='edm3_300'):
		queryList = []
		partitions = self.genQueryPartitions(startTS, endTS)
		for partition in partitions:
			csvYear, csvMonth, csvDay = self.cvtPartitionElements(partition)
			queryString = """SELECT min(dts) as dts, sid, did, ch_cnt, hz
				 FROM %s WHERE dts >= %d AND dts < %d
					 AND csv_year in (%s)
					 AND csv_month in (%s)
					 AND csv_day in (%s)
				 GROUP BY sid, did, ch_cnt, hz
				 ORDER BY sid ASC""" %(tableName, startTS, endTS, csvYear, csvMonth, csvDay)
			queryList.append(queryString)
		queryList = self.stripQueryString(queryList)
		# self._printLog(queryList)
		return queryList
