#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import traceback
import datetime
import calendar
import pytz
import time
import sys
import os

QUARTER_HOUR = 15 * 60 * 1000
ONE_HOUR = 60 * 60 * 1000
ONE_DAY = 24 * 60 * 60 * 1000

# Time Utility
def _convertUnixTimeToUtcTimestamp(unixtime, tz='Asia/Seoul'):
	localTz = pytz.timezone(tz)
	dateformat = None
	if len(unixtime.split('_')) == 2:
		dateformat = "%Y-%m-%d_%H:%M:%S"
	else:
		dateformat = "%Y-%m-%d %H:%M:%S"
	localDateTime = localTz.localize(datetime.datetime.strptime(unixtime, dateformat), is_dst=None)
	timestamp = calendar.timegm(localDateTime.utctimetuple()) * 1000
	return timestamp

def _convertOneHourBaseTimestamp(timestamp):
	base_timestamp = timestamp - (timestamp % ONE_HOUR)
	return base_timestamp

def _convertOneDayBaseTimestamp(timestamp):
	base_timestamp = timestamp - (timestamp % ONE_DAY)
	return base_timestamp

def _convertOneDayBaseTimestampGmt9(timestamp):
	NINE_HOUR = 32400000 #9 * 60 * 60 * 1000
	FIFTINEEN_HOUR = 54000000 #15 * 60 * 60 * 1000

	baseTimestamp = _convertOneDayBaseTimestamp(timestamp)
	if timestamp - baseTimestamp >= FIFTINEEN_HOUR:
		dailyBaseTimestamp = baseTimestamp + FIFTINEEN_HOUR
	else:
		dailyBaseTimestamp = baseTimestamp - NINE_HOUR
	return dailyBaseTimestamp

def getHourlyDatetime():
	currentBTS = _convertOneHourBaseTimestamp(int(time.time() * 1000))
	startBTS = currentBTS - ONE_HOUR*2# - (ONE_HOUR * 9)
	endBTS = currentBTS - 1 - ONE_HOUR# -(ONE_HOUR * 9)
	return startBTS, endBTS

def getDailyDatetime(startTS = None):
	currentBTS = _convertOneDayBaseTimestampGmt9(int(time.time() * 1000))

	startBTS = currentBTS - ONE_DAY
	endBTS = currentBTS - 1
	return startBTS, endBTS

def getPeriodTS(startDT, endDT):
	if startDT.find('_') < 0:
		startDT += '_00:00:00'
	if endDT.find('_') < 0:
		endDT += '_23:59:59'

	try:
		startTS = _convertOneHourBaseTimestamp(_convertUnixTimeToUtcTimestamp(startDT))
		endTS = _convertOneHourBaseTimestamp(_convertUnixTimeToUtcTimestamp(endDT)-1) + ONE_HOUR - 1
	except Exception, e:
		return None, None
	return startTS, endTS


# System config
CONF_FILEPATH = "../conf/"
CONF_NAME = "system.conf"
MYSQL_CONF_LIST = ('host', 'port', 'user', 'password', 'database', 'connectionLimit')

def _getConfig():
	src_path = os.path.dirname(CONF_FILEPATH)
	conf_path = src_path + "/" + CONF_NAME
	if not os.path.exists(conf_path):
		print "# Cannot find collector.ini in conf directory : %s" %src_path
		sys.exit(1)

	conf = ConfigParser.RawConfigParser()
	conf.read(conf_path)
	return conf

def getServerInfo(confName = 'SERVER'):
	conf = _getConfig()
	hostIp = conf.get(confName, 'host')
	hostPort = int(conf.get(confName, 'port'))
	return hostIp, hostPort

def _getSystemEnv(confName = 'ENV'):
	conf = _getConfig()
	systemEnv = conf.get(confName, 'system_env')
	return systemEnv
