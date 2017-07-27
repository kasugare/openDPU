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

def getDebugDataStoreInfo(confName = 'DEBUG_DATA_STORE'):
	conf = getConfig()
	debugDataPath = conf.get(confName, 'path')
	return debugDataPath

def getDpuInfo(confName = 'DPU_INFO'):
	process_config = {}
	conf = getConfig()
	process_config['groupNames'] = conf.get(confName, 'group_names').split(',')
	process_config['metaHistoryPeriod'] = int(conf.get(confName, 'meta_history_period'))
	process_config['metaUpdatePeriod'] = int(conf.get(confName, 'meta_update_period'))
	return process_config

def getDpuGroupNames(confName = 'DPU_INFO'):
	conf = getConfig()
	groupNames = conf.get(confName, 'group_names').split(',')
	return groupNames

def getDpuGroupIds(confName = 'DPU_INFO'):
	conf = getConfig()
	groupIds = conf.get(confName, 'group_ids').replace(' ','').split(',')
	groupIds = [int(groupId) for groupId in groupIds]
	return groupIds

def getDpuMetaSearchPeriod(confName = 'DPU_INFO'):
	conf = getConfig()
	metaHistoryPeriod = (int(conf.get(confName, 'meta_history_period'))) * ONE_DAY
	return metaHistoryPeriod

def getDpuMetaUpdatePeriod(confName = 'DPU_INFO'):
	conf = getConfig()
	metaUpdateTS = (int(conf.get(confName, 'meta_update_period'))) * ONE_DAY
	return metaUpdateTS

def getDpuMetaUpdateDailyPeriod(confName = 'DPU_INFO'):
	conf = getConfig()
	metaUpdateTS = (int(conf.get(confName, 'meta_update_period')))
	return metaUpdateTS

def getAllowedMinPeriod(confName = 'DPU_INFO'):
	conf = getConfig()
	minQuantity = int(conf.get(confName, 'min_quantity'))
	return minQuantity

def getMaxLearningJob(confName = 'DPU_INFO'):
	conf = getConfig()
	maxLearningJob = int(conf.get(confName, 'max_learing_job'))
	return maxLearningJob

def getMaxProcess(confName = 'PROCESSOR'):
	conf = getConfig()
	maxProcess = int(conf.get(confName, 'max_processor'))
	if maxProcess <= 1:
		maxProcess = 2
	return maxProcess

def getPrivateCpu(confName = 'PROCESSOR'):
	maxProcess = getMaxProcess()
	publicCpu = getPublicCpu()
	privateCpu = maxProcess - publicCpu
	return privateCpu

def getPublicCpu(confName = 'PROCESSOR'):
	conf = getConfig()
	publicProcess = int(conf.get(confName, 'max_public_process'))
	maxProcess = getMaxProcess
	if publicProcess >= maxProcess:
		publicProcess = maxProcess -1
	if publicProcess < 0:
		publicProcess = 0
	return publicProcess

def getMlGpu(confName = 'PROCESSOR'):
	conf = getConfig()
	mlGpu = int(conf.get(confName, 'ml_gpu'))
	return mlGpu

def getHeartBeatIntervalTime(confName = 'HEART_BEAT'):
	conf = getConfig()
	interval = int(conf.get(confName, 'interval'))
	return interval

def getMaxRetryCount(confName = 'HEART_BEAT'):
	conf = getConfig()
	maxRetryCnt = int(conf.get(confName, 'max_retry_cnt'))
	return maxRetryCnt

def getJobRetryCount(confName = 'JOBS'):
	conf = getConfig()
	maxRetryCnt = int(conf.get(confName, 'max_retry_cnt'))
	return maxRetryCnt
