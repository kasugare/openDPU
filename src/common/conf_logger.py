#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import sys
import os

CONF_PATH = "../conf/"
CONF_FILENAME = "logger.ini"

LOGGER_INFO = ('log_name', 'log_level', 'log_format', 'log_dir', 'log_filename')
HANDLER_INFO = ('is_stream', 'is_file')
LOTATE_INFO = ('is_lotate', 'log_maxsize', 'backup_count')

def _getConfig():
	src_path = os.path.dirname(CONF_PATH)
	ini_path = src_path + "/" + CONF_FILENAME
	if not os.path.exists(ini_path):
		print "# Cannot find logger.ini in conf directory : %s" %src_path
		sys.exit(1)

	conf = ConfigParser.RawConfigParser()
	conf.read(ini_path)
	return conf

def _cvtFlag(value):
	if value.lower() == 'true':
		return True
	elif value.lower() == 'false':
		return False
	else:
		print "# Wrong logger values in logger.ini."
		sys.exit(1)

def _checkInteger(value):
	try:
		return int(value)
	except Exception, e:
		print "# Wrong logger values in logger.ini."
		sys.exit(1)

def _getLoggerConf(configList, elemnts = LOGGER_INFO, confName = 'MASTER_LOGGER'):
	conf = _getConfig()
	for elementName in elemnts:
		configList[elementName] = conf.get(confName, elementName)
	return configList

def _getHandlerConf(configList, elemnts = HANDLER_INFO, confName = 'HANDLER'):
	conf = _getConfig()
	for elementName in elemnts:
		elementValue = conf.get(confName, elementName)
		if elementName == 'is_stream':
			configList[elementName] = _cvtFlag(elementValue)
		elif elementName == 'is_file':
			configList[elementName] = _cvtFlag(elementValue)
		else:
			configList[elementName] = elementValue
	return configList

def _getLotateConf(configList, elemnts = LOTATE_INFO, confName = 'LOTATE'):
	conf = _getConfig()
	for elementName in elemnts:
		elementValue = conf.get(confName, elementName)
		if elementName == 'is_lotate':
			configList[elementName] = _cvtFlag(elementValue)
		elif elementName == 'log_maxsize':
			configList[elementName] = _checkInteger(elementValue)
		elif elementName == 'backup_count':
			configList[elementName] = _checkInteger(elementValue)
		else:
			configList[elementName] = elementValue
	return configList

def getLoggerInfo(loggerConfName):
	configList = {}
	configList = _getLoggerConf(configList, LOGGER_INFO, loggerConfName)
	configList = _getHandlerConf(configList)
	configList = _getLotateConf(configList)
	return configList
