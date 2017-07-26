#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import sys
import os

CONF_PATH = "../conf/"
CONF_FILENAME = "nilm_groups.ini"

def getConfig():
	src_path = os.path.dirname(CONF_PATH)
	ini_path = src_path + "/" + CONF_FILENAME
	if not os.path.exists(ini_path):
		print "# Cannot find nilm_core.ini in conf directory : %s" %src_path
		sys.exit(1)

	conf = ConfigParser.RawConfigParser()
	conf.read(ini_path)
	return conf

def getNilmGroupTable(logger=None, confName='NILM_GROUP_TABLE'):
	conf = getConfig()
	nilmGroupTable = {}
	optionInfos = conf.items(confName)
	if optionInfos:
		for optionInfo in optionInfos:
			try:
				groupName = optionInfo[0].upper()
				groupId = int(optionInfo[1])
				nilmGroupTable[groupName] = groupId
			except:
				if logger:
					logger.warn('check configuration of nilm group table in "conf/nilm_groups.ini"')
				else:
					print 'check configuration of nilm group table in "conf/nilm_groups.ini"'
	return nilmGroupTable

def getNilmGroupInfoForClient(logger=None, confName='NILM_GROUP_TABLE'):
	conf = getConfig()
	nilmGroupTable = {}
	optionInfos = conf.items(confName)
	if optionInfos:
		for optionInfo in optionInfos:
			try:
				groupName = optionInfo[0]
				groupId = int(optionInfo[1])
				nilmGroupTable[groupId] = groupName
			except:
				if logger:
					logger.warn('check configuration of nilm group table in "conf/nilm_groups.ini"')
				else:
					print 'check configuration of nilm group table in "conf/nilm_groups.ini"'
	return nilmGroupTable
