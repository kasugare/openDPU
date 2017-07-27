#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_system import getDpuMetaSearchPeriod
import traceback
import datetime
import calendar
import pytz
import time

QUARTER_HOUR = 15 * 60 * 1000
ONE_HOUR = 60 * 60 * 1000
ONE_DAY = 24 * 60 * 60 * 1000


def convert15minBaseTimestamp(timestamp):
	base_timestamp = timestamp - (timestamp % QUARTER_HOUR)
	return base_timestamp

def convertOneHourBaseTimestamp(timestamp):
	base_timestamp = timestamp - (timestamp % ONE_HOUR)
	return base_timestamp

def convertOneDayBaseTimestamp(timestamp):
	base_timestamp = timestamp - (timestamp % ONE_DAY)
	return base_timestamp

def convertOneDayBaseTimestampGmt9(timestamp):
	NINE_HOUR = 32400000 #9 * 60 * 60 * 1000
	FIFTINEEN_HOUR = 54000000 #15 * 60 * 60 * 1000

	baseTimestamp = convertOneDayBaseTimestamp(timestamp)
	if timestamp - baseTimestamp >= FIFTINEEN_HOUR:
		dailyBaseTimestamp = baseTimestamp + FIFTINEEN_HOUR
	else:
		dailyBaseTimestamp = baseTimestamp - NINE_HOUR
	return dailyBaseTimestamp

def convertUnixTimeToUtcTimestamp(unixtime, tz='Asia/Seoul'):
	localTz = pytz.timezone(tz)
	dateformat = None
	if len(unixtime.split('_')) == 2:
		dateformat = "%Y-%m-%d_%H:%M:%S"
	else:
		dateformat = "%Y-%m-%d %H:%M:%S"
	localDateTime = localTz.localize(datetime.datetime.strptime(unixtime, dateformat), is_dst=None)
	timestamp = calendar.timegm(localDateTime.utctimetuple()) * 1000
	return timestamp

def convertDailyBaseTimestampToGmt9(timestamp):
	NINE_HOUR = 32400000 #9 * 60 * 60 * 1000
	FIFTINEEN_HOUR = 54000000 #15 * 60 * 60 * 1000

	baseTimestamp = convertOneDayBaseTimestamp(timestamp)
	if timestamp - baseTimestamp >= FIFTINEEN_HOUR:
		dailyBaseTimestamp = baseTimestamp + FIFTINEEN_HOUR
	else:
		dailyBaseTimestamp = baseTimestamp - NINE_HOUR
	date = datetime.datetime.fromtimestamp(dailyBaseTimestamp/1e3, pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
	return date

def getOneHourAgoTimestamp():
	dt = datetime.datetime.utcnow() + datetime.timedelta(hours=-1)
	gmt_timestamp = calendar.timegm(dt.utctimetuple()) * 1000
	startTS = convertOneHourBaseTimestamp(gmt_timestamp)
	endTS = startTS + ONE_HOUR - 1
	return startTS, endTS

def getQHAgoTimestamp():
	dt = datetime.datetime.utcnow() + datetime.timedelta(minute=-15)
	gmt_timestamp = calendar.timegm(dt.utctimetuple()) * 1000
	startTS = convert15minBaseTimestamp(gmt_timestamp)
	endTS = startTS + QUARTER_HOUR - 1
	return startTS, endTS

def getPeriodTS(startDT, endDT):
	try:
		startTS = convertOneHourBaseTimestamp(convertUnixTimeToUtcTimestamp(startDT))
		endTS = convertOneHourBaseTimestamp(convertUnixTimeToUtcTimestamp(endDT)-1) + ONE_HOUR - 1
	except Exception, e:
		return None, None
	return startTS, endTS

def getHourlyDatetime():
	currentBTS = convertOneHourBaseTimestamp(int(time.time() * 1000))
	startBTS = currentBTS - ONE_HOUR * 2# - (ONE_HOUR * 9)
	endBTS = currentBTS - 1 - ONE_HOUR# -(ONE_HOUR * 9)
	return startBTS, endBTS

def getDailyDatetime(startTS = None):
	currentBTS = convertOneDayBaseTimestampGmt9(int(time.time() * 1000))
	startBTS = currentBTS - ONE_DAY
	endBTS = currentBTS - 1
	return startBTS, endBTS

def getUtcNowTS():
	dt = datetime.datetime.utcnow()
	utc_timestamp = calendar.timegm(dt.utctimetuple())
	return utc_timestamp

def htime(timestamp):
	date = datetime.datetime.fromtimestamp(timestamp/1e3, pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
	return date

def tzHtime(timestamp, tz='Asia/Seoul'):
	date = datetime.datetime.fromtimestamp(timestamp/1e3, pytz.timezone(tz)).strftime('%Y-%m-%d %H:%M:%S')
	return date

def convertTS2Date(timestamp, tz='Asia/Seoul'):
	date = datetime.datetime.fromtimestamp(timestamp/1e3, pytz.timezone(tz)).strftime('%Y-%m-%d')
	return date

def convertTS2DailyDateList(startTS, endTS):
	dailyDateList = []
	while True:
		if startTS <= endTS:
			dailyDateList.append(convertTS2Date(startTS))
		else:
			break
		startTS += ONE_DAY
	return dailyDateList

def convertTS2DailyTimestampList(startTS, endTS):
	dailyDateList = []
	while True:
		startTS = convertOneDayBaseTimestampGmt9(startTS)
		endTS = convertOneDayBaseTimestampGmt9(endTS)
		if startTS <= endTS:
			dailyDateList.append(startTS)
		else:
			break
		startTS += ONE_DAY
	return dailyDateList

def isDpuUpdatale(updateTS, dpuMetaSearchPeriod = None):
	if not dpuMetaSearchPeriod:
		dpuMetaSearchPeriod = getDpuMetaSearchPeriod()

	currentTS = getUtcNowTS()
	if (currentTS - updateTS) >= dpuMetaSearchPeriod:
		return True
	return False

def getDailyPeriodicTS(startTS):
	fromTS = convertOneDayBaseTimestampGmt9(startTS)
	toTS = convertOneDayBaseTimestampGmt9(startTS) + 86400000 -1
	return fromTS, toTS

def getDailyPeriodSet(startTS, endTS):
	fromTS, toTS = getDailyPeriodicTS(startTS)
	dailyPeriodSet = [[fromTS, toTS]]
	while toTS < endTS:
		fromTS, toTS = getDailyPeriodicTS(toTS+1)
		dailyPeriodSet.append([fromTS, toTS])

	return dailyPeriodSet

def cvtNextDayOfTS(nextDay):
	nextDayOfTS = getUtcNowTS() * 1000 + ONE_DAY * nextDay
	nextDayOfBTS = convertOneDayBaseTimestamp(nextDayOfTS)
	return nextDayOfBTS

def cvtDayOfTS(day):
	dayOfTS = ONE_DAY * day
	return dayOfTS
