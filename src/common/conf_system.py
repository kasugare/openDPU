#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import traceback
import sys
import os

CONF_PATH = "../conf/"
CONF_FILENAME = "system.ini"

LOG_CONF_LIST = ('path', 'level')
PROCESS_CONF_LIST = ('host', 'port', 'num_of_process', 'daemon', 'assign_interval_time')

ONE_DAY = 24 * 60 * 60 * 1000

def getConfig():
	src_path = os.path.dirname(CONF_PATH)
	ini_path = src_path + "/" + CONF_FILENAME
	if not os.path.exists(ini_path):
		print "# Cannot find system.ini in conf directory : %s" %src_path
		sys.exit(1)

	conf = ConfigParser.RawConfigParser()
	conf.read(ini_path)
	return conf

def getHomeDir(confName = 'HOME'):
	conf = getConfig()
	homeDir = conf.get(confName, 'home_dir')
	return homeDir

def getLogDir(confName = 'LOGGER'):
	conf = getConfig()
	logPath = "%s%s" %(getHomeDir(), conf.get(confName, 'path'))
	logLevel = conf.get(confName, 'level')
	return logPath, logLevel

def getLogLevel(confName = 'LOGGER'):
	conf = getConfig()
	logLevel = conf.get(confName, 'level')
	return logLevel

def getSystemEnv(confName = 'ENV'):
	conf = getConfig()
	systemEnv = conf.get(confName, 'system_env')
	return systemEnv

def getProcessInfo(confName = 'PROCESS_MANAGER'):
	process_config = {}
	conf = getConfig()
	for element_name in PROCESS_CONF_LIST:
		if element_name == 'daemon':
			if str(conf.get(confName, element_name)).upper() == 'TRUE':
				process_config[element_name] = True
			else:
				process_config[element_name] = False
		else:
			process_config[element_name] = conf.get(confName, element_name)
	return process_config

def getDataStoreInfo(confName = 'DATA_STORE'):
	conf = getConfig()
	dataPath = conf.get(confName, 'path')
	return dataPath

def getNilmInfo(confName = 'NILM_INFO'):
	process_config = {}
	conf = getConfig()
	process_config['groupNames'] = conf.get(confName, 'group_names').split(',')
	process_config['metaHistoryPeriod'] = int(conf.get(confName, 'meta_history_period'))
	process_config['metaUpdatePeriod'] = int(conf.get(confName, 'meta_update_period'))
	return process_config

def getNilmGroupNames(confName = 'NILM_INFO'):
	conf = getConfig()
	groupNames = conf.get(confName, 'group_names').split(',')
	return groupNames

def getNilmMetaSearchPeriod(confName = 'NILM_INFO'):
	conf = getConfig()
	metaHistoryPeriod = (int(conf.get(confName, 'meta_history_period')) -1) * ONE_DAY
	return metaHistoryPeriod

def getNilmMetaUpdatePeriod(confName = 'NILM_INFO'):
	conf = getConfig()
	metaUpdateTS = (int(conf.get(confName, 'meta_update_period')) -1 ) * ONE_DAY
	return metaUpdateTS

def getMaxProcess(confName = 'PROCESSOR'):
	conf = getConfig()
	maxProcess = int(conf.get(confName, 'max_processor'))
	return maxProcess

def getHeartBeatIntervalTime(confName = 'HEART_BEAT'):
	conf = getConfig()
	interval = int(conf.get(confName, 'interval'))
	return interval

def getMaxRetryCount(confName = 'HEART_BEAT'):
	conf = getConfig()
	maxRetryCnt = int(conf.get(confName, 'max_retry_cnt'))
	return maxRetryCnt

def getHdfsInfo(confName = 'HDFS'):
	conf = getConfig()
	hostUrl = conf.get(confName, 'hostUrl')
	user = conf.get(confName, 'user')
	return hostUrl, user