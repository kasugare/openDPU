# -*- coding: utf-8 -*-

import ConfigParser
import traceback
import sys
import os

CONF_PATH = "../conf/"
CONF_FILENAME = "collector.ini"

TAJO_GATEWAY_LIST = ('host', 'port', 'buffer')
SCHEMA_CONF_LIST = ('table', 'channels', 'common', 'columns', 'recoverable')

def getConfig():
	src_path = os.path.dirname(CONF_PATH)
	ini_path = src_path + "/" + CONF_FILENAME
	if not os.path.exists(ini_path):
		print "# Cannot find collector.ini in conf directory : %s" %src_path
		sys.exit(1)

	conf = ConfigParser.RawConfigParser()
	conf.read(ini_path)
	return conf

def getAllowedProtocol(confName = 'ALLOWED_DEVICE_PROTOCOL', field_name = 'device_protocol'):
	conf = getConfig()
	deviceProtocols = conf.get(confName, field_name).split(',')
	return deviceProtocols

def getDataCollector(confName = 'USAGE_COLLECTOR'):
	conf = getConfig()
	hostIp = conf.get(confName, 'host')
	hostPort = int(conf.get(confName, 'port'))
	return hostIp, hostPort

def getBulkSize(confName = 'USAGE_COLLECTOR'):
	conf = getConfig()
	bulkSize = int(conf.get(confName, 'bulk_size'))
	return bulkSize

def getHdfsTmpPath(confName = 'TEMP_FILE_PATH'):
	conf = getConfig()
	hdfsTmpPath = conf.get(confName, 'hdfs_tmp_path').split(',')
	return hdfsTmpPath

def getTajoTmpPath(confName = 'TEMP_FILE_PATH'):
	conf = getConfig()
	tajoTmpPath = conf.get(confName, 'tajo_tmp_path').split(',')[0]
	return tajoTmpPath

def getLocalTmpPath(confName = 'TEMP_FILE_PATH'):
	conf = getConfig()
	localTmpPath = conf.get(confName, 'local_tmp_path').split(',')[0]
	return localTmpPath

def getHdfsDpuPath(confName = 'DPU_RAWDATA_PATH'):
	conf = getConfig()
	hdfsDpuPath = conf.get(confName, 'hdfs_dpu_path').split(',')[0]
	return hdfsDpuPath

def getHdfsMlWeightDirPath(confName = 'DPU_ML_WEIGHT'):
	conf = getConfig()
	hdfsDpuPath = conf.get(confName, 'hdfs_weight_path').split(',')[0]
	return hdfsDpuPath

def getHdfsMlTempDirPath(confName = 'DPU_ML_TEMP_PATH'):
	conf = getConfig()
	mlTempDirPath = conf.get(confName, 'ml_temp_path').split(',')[0]
	return mlTempDirPath

def getTajoGateway(confName = 'TAJO_GATEWAY'):
	tajoGatewayConf = {}
	conf = getConfig()
	for elementName in TAJO_GATEWAY_LIST:
		tajoGatewayConf[elementName] = conf.get(confName, elementName)
	return tajoGatewayConf

def getTajoQueryFailorverOptions(confName = 'TAJO_QUERY_OPTIONS'):
	conf = getConfig()
	retryQueryCount = int(conf.get(confName, 'retry_query_execution_count'))
	sleepTime = int(conf.get(confName, 'sleep_seconds'))
	return retryQueryCount, sleepTime

