#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from client_listener.ClientConnectionServer import ClientConnectionServer
from collection_manager.DataCollectionManager import DataCollectionManager
from resource_handler.MasterResourceHandler import MasterResourceHandler
from job_manager.JobManager import JobManager
from ClusterMaster import ClusterMaster
from threading import Thread, Lock
import multiprocessing

PROCESS_NAME = 'MASTER'

class NilmClusterMaster(ClusterMaster):
	def __init__(self):
		ClusterMaster.__init__(self)
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('---------- [ NILM Cluster Master ] ----------')
		self._resourceHandler = MasterResourceHandler(self._logger)
		self._clientRequestJobQueue = multiprocessing.Queue()
		self._clientResponseJobQueue = multiprocessing.Queue()
		self._initProcess()

	def __del__(self):
		self._logger.info('# NILM Cluster Master Terminate...')
		self._logger.info('---------------------------------------------')

	def _initProcess(self):
		collectorProcess = Thread(target=self._initCollectionManager, args=())
		collectorProcess.setDaemon(1)
		collectorProcess.start()

		jobProcess = Thread(target=self._initJobManager, args=())
		jobProcess.setDaemon(1)
		jobProcess.start()

	def _initCollectionManager(self):
		collectorManager = DataCollectionManager(self._resourceHandler, self._clientRequestJobQueue)
		collectorManager.doProcess()

	def _initJobManager(self):
		try:
			jobManager = JobManager(self._resourceHandler, self._clientResponseJobQueue)
			jobManager.runServer()
		except Exception, e:
			self._logger.exception(e)

	def doProcess(self):
		try:
			clientServer = ClientConnectionServer(self._logger, self._resourceHandler, self._clientRequestJobQueue, self._clientResponseJobQueue)
			clientServer.runServer()
		except Exception, e:
			self._logger.exception(e)
