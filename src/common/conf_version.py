#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import traceback
import sys
import os

CONF_PATH = "../conf/"
CONF_FILENAME = "system.ini"

def _getVerionsInfoFilePath(confName = 'HOME'):
	src_path = os.path.dirname("../conf/")
	ini_path = os.path.join(src_path, "system.ini")
	if not os.path.exists(ini_path):
		print "# Cannot find system.ini in conf directory : %s" %src_path
		sys.exit(1)

	conf = ConfigParser.RawConfigParser()
	conf.read(ini_path)
	homeDir = conf.get(confName, 'home_dir')
	versionFilePath = os.path.join(homeDir, 'VERSION')
	return versionFilePath

def getConfig():
	versionFilePath = _getVerionsInfoFilePath()
	if not os.path.exists(versionFilePath):
		print "# Cannot find VERSION file in home directory : %s" %(versionFilePath)
		return None

	conf = ConfigParser.RawConfigParser()
	conf.read(versionFilePath)
	return conf

def getVersionInfo(confName = 'VERSION'):
	conf = getConfig()
	if conf:
		versionInfo = {}
		for configOption in conf.items(confName):
			optionName = configOption[0]
			optionValue = configOption[1]
			versionInfo[optionName] = optionValue
		return versionInfo
	return None
