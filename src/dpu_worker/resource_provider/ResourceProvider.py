#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from common.conf_network import getHostInfo
from common.util_protocol_parser import parseProtocol
from dpu_worker.resource_provider.WorkerResource import WorkerResource
from message_pool.system_protocol_messages import genResWorkerResource
from network.NetworkHandler import NetworkHandler
from threading import Thread, Lock
import multiprocessing
import time

PROCESS_NAME = 'RESOURCE_PROVIDER'

class ResourceProvider(WorkerResource):
	def __init__(self, resourceQ):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('# process start : %s' %PROCESS_NAME)
		self._reqQ = resourceQ['reqQ']
		self._routerQ = resourceQ['routerQ']
		self._resourceQ = multiprocessing.Queue()
		self._networkObj = None

		WorkerResource.__init__(self, self._logger)

	def __del__(self):
		self._logger.warn('@ terminate process : %s' %PROCESS_NAME)
		self.clearResource()

	def doProcess(self):
		try:
			while True:
				reqMsg = self._reqQ.get()
				print "[Q] ResourceProvider :", reqMsg
				protocol, statCode = parseProtocol(reqMsg)
				if protocol == 'SYS_REQ_RESOURCE':
					self._routerQ.put_nowait(genResWorkerResource(reqMsg['workerId'], self.getCurrentResource()))

		except KeyboardInterrupt, e:
			pass
