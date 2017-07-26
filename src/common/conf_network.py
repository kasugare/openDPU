# -*- coding: utf-8 -*-

import ConfigParser
import traceback
import sys
import os

CONF_PATH = "../conf/"
CONF_FILENAME = "network.ini"

def getConfig():
	src_path = os.path.dirname(CONF_PATH)
	ini_path = src_path + "/" + CONF_FILENAME
	if not os.path.exists(ini_path):
		print "# Cannot find collector.ini in conf directory : %s" %src_path
		sys.exit(1)

	conf = ConfigParser.RawConfigParser()
	conf.read(ini_path)
	return conf

def getHostInfo(confName = 'MASTER'):
	conf = getConfig()
	hostIp = conf.get(confName, 'host')
	hostPort = int(conf.get(confName, 'port'))
	return hostIp, hostPort

def getWorkerStubInfo(confName = 'WORKER_STUB'):
	conf = getConfig()
	hostIp = conf.get(confName, 'host')
	hostPort = int(conf.get(confName, 'port'))
	return hostIp, hostPort
