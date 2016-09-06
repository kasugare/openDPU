#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from common.util_protocol_parser import parseProtocol
from dpu_master.resource_manager.Resources import Resources
from dpu_master.resource_manager.ResourceServer import ResourceServer
from threading import Thread, Lock
import multiprocessing
import time

PROCESS_NAME = 'RESOURCE_MASTER'

class ResourceMaster(Resources):
	def __init__(self, resourceQ):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('# process start : %s' %PROCESS_NAME)
		self._reqQ = resourceQ['reqQ']
		self._routerQ = resourceQ['routerQ']
		Resources.__init__(self, self._logger)

	def __del__(self):
		self._logger.warn('@ terminate process : %s' %PROCESS_NAME)

	def doProcess(self):
		try:
			while True:
				reqMsg = self._routerQ.get()
				print "[Q] ResourceMaster :", reqMsg
				protocol, statCode = parseProtocol(reqMsg)

				if protocol == 'SYS_SET_RESOURCE':
					self.initResource(reqMsg['workerId'], reqMsg['resource'])
				elif protocol == 'SYS_DEL_RESOURCE':
					self.delResource(reqMsg['workerId'])
				print '[RESOURCE] resource size : %d' %(len(self._resources))
		except KeyboardInterrupt, e:
			pass
