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
		print "# Cannot find hdfs_site.ini in conf directory : %s" %src_path
		sys.exit(1)

	conf = ConfigParser.RawConfigParser()
	conf.read(ini_path)
	return conf

def getHdfsInfo(confName = 'HDFS'):
	conf = getConfig()
	hdfsIps = conf.get(confName, 'hdfs_hosts').replace(' ','').split(',')
	hdfsPort = conf.get(confName, 'hdfs_port')
	hdfsHosts = []
	for hdfsIp in hdfsIps:
		hdfsHosts.append('http://%s:%s' %(hdfsIp, hdfsPort))

	user = conf.get(confName, 'user')
	return hdfsHosts, user


def getHdfsDpuPath(confName = 'DPU_RAWDATA_PATH'):
	conf = getConfig()
	hdfsDpuPath = conf.get(confName, 'hdfs_dpu_path').split(',')[0]
	return hdfsDpuPath
