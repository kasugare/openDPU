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

def getHdfsTmpPath(confName = 'TEMP_FILE_PATH'):
	conf = getConfig()
	hdfsTmpPath = conf.get(confName, 'hdfs_tmp_path').split(',')
	return hdfsTmpPath

def getLocalTmpPath(confName = 'TEMP_FILE_PATH'):
	conf = getConfig()
	localTmpPath = conf.get(confName, 'local_tmp_path').split(',')[0]
	return localTmpPath

def getHdfsNilmPath(confName = 'NILM_RAWDATA_PATH'):
	conf = getConfig()
	hdfsNilmPath = conf.get(confName, 'hdfs_nilm_path').split(',')[0]
	return hdfsNilmPath

def getHdfsURL(confName = 'HDFS'):
	conf = getConfig()
	hdfsHost = conf.get(confName, 'host')
	hdfsPort = conf.get(confName, 'port')
	hdfsUser = conf.get(confName, 'user')
	hdfsUrl = '%s:%s' %(hdfsHost, hdfsPort)
	return hdfsUrl, hdfsUser