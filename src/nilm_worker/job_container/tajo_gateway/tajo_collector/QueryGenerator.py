#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
import pytz


DAILY_TS = 86400000
HOURLY_TS = 3600000
QH_TS = 900000

class QueryGenerator:
	def genQueryPartitions(self, startTS, endTS):
		def _cvtDailyElements(timestamp):
			date = str(datetime.fromtimestamp(int(timestamp)/1e3, pytz.utc).strftime('%Y-%m-%d'))
			return date

		startTS -= QH_TS
		endTS += QH_TS
		startDate = _cvtDailyElements(startTS)
		endDate = _cvtDailyElements(endTS)
		dateRange = [startDate]

		if startDate == endDate:
			return dateRange
		else:
			dateRange.append(endDate)

		dateDiffRange = (endTS - startTS)/DAILY_TS
		for dayCount in range(dateDiffRange):
			targetTS = startTS + DAILY_TS * (dayCount + 1)
			date = _cvtDailyElements(targetTS)
			dateRange.append(date)
		dateRange = list(set(dateRange))

		inRange = {}
		for date in dateRange:
			splitDate = date.split('-')
			monthlyKey = '%s-%s' %(splitDate[0], splitDate[1])
			if inRange.has_key(monthlyKey):
				inRange[monthlyKey].append(splitDate[2])
			else:
				inRange[monthlyKey] = [splitDate[2]]

		dateRange = []
		for monthlyKey in inRange.keys():
			dateRange.append("%s-%s" %(monthlyKey, ",".join(inRange[monthlyKey])))
		return dateRange

	def cvtPartitionElements(self, partition):
		splitPartition = partition.split('-')
		csvYear = splitPartition[0].split(',')
		csvMonth = splitPartition[1].split(',')
		csvDays = splitPartition[2].split(',')
		csvYear.sort()
		csvMonth.sort()
		csvDays.sort()

		partitionYear = ','.join(["""'%s'""" %year for year in csvYear])
		partitionMonth = ','.join(["""'%s'""" %month for month in csvMonth])
		partitionDay = ','.join(["""'%s'""" %day for day in csvDays])
		return partitionYear, partitionMonth, partitionDay

	def cvtPartitionElement(self, partitions):
		csvYearList = []
		csvMonthList = []
		csvDayList = []
		for partition in partitions:
			splitPartition = partition.split('-')
			csvYear = splitPartition[0].split(',')
			csvMonth = splitPartition[1].split(',')
			csvDays = splitPartition[2].split(',')
			csvYearList += csvYear
			csvMonthList += csvMonth
			csvDayList += csvDays

		csvYearList = list(set(csvYearList))
		csvMonthList = list(set(csvMonthList))
		csvDayList = list(set(csvDayList))

		csvYearList.sort()
		csvMonthList.sort()
		csvDayList.sort()

		partitionYear = ','.join(["""'%s'""" %year for year in csvYearList])
		partitionMonth = ','.join(["""'%s'""" %month for month in csvMonthList])
		partitionDay = ','.join(["""'%s'""" %day for day in csvDayList])
		return partitionYear, partitionMonth, partitionDay


	def addInsertLocation(self, hdfsTmpPath, queryString):
		if hdfsTmpPath:
			queryString = """INSERT INTO LOCATION \'%s\' %s""" %(hdfsTmpPath, queryString)
			return queryString
		return queryString

	def addSidDidParams(self, sids):
		params = ''
		if '*' not in sids:
			sids = [str(sid) for sid in sids]
			params = ' AND sid in (%s)' %(','.join(sids))
		return params

	def addParams(self, queryList, sids):
		if '*' not in sids:
			sids = [str(sid) for sid in sids]
			for index in range(len(queryList)):
				queryString = queryList[index]
				queryString = queryString + ' AND sid in (%s)' %(','.join(sids))
				queryList[index] = queryString

		for index in range(len(queryList)):
			queryString = queryList[index]
			queryList[index] = queryString.replace("\n","").replace("\t","")
		return queryList

	def addOrderby(self, queryList):
		for query in queryList:
			index = queryList.index(query)
			queryList[index] = '%s ORDER BY sid ASC' %query
		return queryList

	def addOrderbySidDts(self, queryList):
		for query in queryList:
			index = queryList.index(query)
			queryList[index] = '%s ORDER BY sid, dts ASC' %query
		return queryList

	def addEdm1QueryOptions(self, queryList):
		OPTIONS = " GROUP BY bts, did, sid, sp  ORDER BY did, bts ASC; "
		for index in range(len(queryList)):
			queryString = queryList[index]
			queryString += OPTIONS
			queryList[index] = queryString
		return queryList

	def addEdm3QueryOptions(self, queryList):
		OPTIONS = " GROUP BY bts, did, sid  ORDER BY did, bts ASC; "
		for index in range(len(queryList)):
			queryString = queryList[index]
			queryString += OPTIONS
			queryList[index] = queryString
		return queryList

	def stripQueryString(self, queryList):
		for index in range(len(queryList)):
			queryString = queryList[index]
			queryList[index] = queryString.replace("\n","").replace("\t","")
		return queryList
