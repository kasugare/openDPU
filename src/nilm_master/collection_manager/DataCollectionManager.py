#!/usr/bin/env python
# -*- coding: utf-8 -*-

from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from protocol.MessageRouter import MessageRouter
from nilm_master.collection_manager.JobRoutingHandler import JobRoutingHandler
from common.conf_network import getHostInfo
from common.util_logger import Logger
from threading import Thread, Lock

PROCESS_NAME = 'DATA_COLLECTOR'

class DataCollectionManager:
	def __init__(self, resourceHandler, clientRequestJobQueue):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._resourceHandler = resourceHandler
		self._clientRequestJobQueue = clientRequestJobQueue
		self._logger.info('-------- [ Data Collection Manager ] --------')
		self._logger.info("# Start data collection management server.")

	def __del__(self):
		self._logger.info('# Data Collection Manager Terminate...')
		self._logger.info('-----------------------------------------')

	def doProcess(self):
		JobRoutingHandler(self._logger, self._resourceHandler, self._clientRequestJobQueue).doProcess()
