#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
import pytz

DAILY_TS = 86400000
HOURLY_TS = 3600000
QH_TS = 900000

class TajoQueryHelper:

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

	def genQueryPartitions(self, startTS, endTS):
		def cvtDailyElements(timestamp):
			date = str(datetime.fromtimestamp(int(timestamp)/1e3, pytz.utc).strftime('%Y-%m-%d'))
			return date

		startTS -= QH_TS
		endTS += QH_TS
		startDate = cvtDailyElements(startTS)
		endDate = cvtDailyElements(endTS)
		dateRange = [startDate]

		if startDate == endDate:
			return dateRange
		else:
			dateRange.append(endDate)

		dateDiffRange = (endTS - startTS)/DAILY_TS
		for dayCount in range(dateDiffRange):
			targetTS = startTS + DAILY_TS * (dayCount + 1)
			date = cvtDailyElements(targetTS)
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

	def stripQueryString(self, queryList):
		for index in range(len(queryList)):
			queryString = queryList[index]
			queryList[index] = queryString.replace("\n","").replace("\t","")
		return queryList