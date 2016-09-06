#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_system import getSystemEnv
import ConfigParser
import sys
import os

CONF_FILENAME = "../conf/"
CONF_NAME = "datastore.ini"

MYSQL_CONF_LIST = ('host', 'port', 'user', 'password', 'database', 'connectionLimit')
MYSQL_EXECUTE_SIZE = ('qh_feeder', 'hourly_feeder', 'daily_feeder')
CASSANDRA_CONF_LIST = ('username', 'password', 'contact_points', 'port', 'keyspace', 'executor_thread')
CASSANDRA_STAGING_CONF_LIST = ('contact_points', 'port', 'keyspace', 'executor_thread')


def getConfig():
	src_path = os.path.dirname(CONF_FILENAME)
	ini_path = src_path + "/" + CONF_NAME
	if not os.path.exists(ini_path):
		print "# Cannot find datastore.ini in conf directory : %s" %src_path
		sys.exit(1)

	conf = ConfigParser.RawConfigParser()
	conf.read(ini_path)
	return conf

def getDBInfo(executor_name = 'MYSQL'):
	env = getSystemEnv()
	if env == 'production':
		executor_name = 'CONFIG_MYSQL'
	else:
		executor_name = 'CONFIG_MYSQL_STAGING'

	db_config = {}
	conf = getConfig()
	for element_name in MYSQL_CONF_LIST:
		db_config[element_name] = conf.get(executor_name, element_name)
	return db_config

def getETableExecutorInfo(executor_name = 'CASSANDRA'):
	env = getSystemEnv()
	if env == 'staging':
		executor_name = 'CASSANDRA_STAGING'
		CONF_LIST = CASSANDRA_STAGING_CONF_LIST
	else:
		CONF_LIST = CASSANDRA_CONF_LIST

	executor_config = {}
	conf = getConfig()
	for element_name in CONF_LIST:

		executor_config[element_name] = conf.get(executor_name, element_name)
		
	if 'contact_points' in executor_config.keys():
		executor_config['contact_points'] = str(executor_config['contact_points']).split(',')

	if 'executor_thread' in executor_config.keys():
		executor_config['executor_thread'] = int(executor_config['executor_thread'])

	if 'port' in executor_config.keys():
		executor_config['port'] = int(executor_config['port'])
	return executor_config

def getETableExecutionSizeInfo(executor_name = 'CASSANDRA'):
	conf = getConfig()
	execution_size = int(conf.get(executor_name, 'execution_size'))
	return execution_size

def getDbExecutableSize(conf_name = 'MYSQL_INSERT_SIZE'):
	conf = getConfig()
	dbExecutableSize = {}
	for elements_name in MYSQL_EXECUTE_SIZE:
		dbExecutableSize[elements_name] = conf.get(conf_name, elements_name)
	return dbExecutableSize
