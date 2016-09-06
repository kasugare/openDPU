#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import sys
import os

CONF_PATH = "../conf/"
CONF_FILENAME = "hdfs_site.ini"

def getConfig():
	src_path = os.path.dirname(CONF_PATH)
	ini_path = src_path + "/" + CONF_FILENAME
	if not os.path.exists(ini_path):
		print "# Cannot find system.ini in conf directory : %s" %src_path
		sys.exit(1)

	conf = ConfigParser.RawConfigParser()
	conf.read(ini_path)
	return conf

def getHdfsInfo(confName = 'HDFS'):
	conf = getConfig()
	hdfsHost = conf.get(confName, 'hdfs_host')
	user = conf.get(confName, 'user')
	return hdfsHost, user
