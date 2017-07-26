#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tcp_modules.NetworkHandler import DataSender
from threading import Thread, Lock
import multiprocessing

class StubJobRouter:
	def __init__(self, logger, resourceHandler, jobRequestQ, stubRequestQ, stubResultQ, stubMappingTable):
		self._logger = logger
		self._resourceHandler = resourceHandler
		self._stubMappingTable = stubMappingTable
		self._jobRequestQ = jobRequestQ
		self._stubRequestQ = stubRequestQ
		self._stubResultQ = stubResultQ
		self._initStubResultHandler()
		
	def _initStubResultHandler(self):
		outputRoutingThread = Thread(target=self.runResultHandler, args=())
		outputRoutingThread.setDaemon(1)
		outputRoutingThread.start()

	def runJobRequest(self):
		while True:
			try:
				message = self._stubRequestQ.get()
				if type(message) is not dict or not message.has_key('networkId'):
					continue
				else:
					message['stubId'] = message['networkId']
					
				protocol = message['proto']
				if protocol == 'REQ_STUB_NILM_ML':
					mlProcess = self._resourceHandler.getMlProcess()
					message['processInfo'] = mlProcess
					self._jobRequestQ.put_nowait(message)
				elif protocol == 'REQ_HB':
					pass
			except Exception, e:
				self._logger.exception(e)

	def runResultHandler(self):
		def putMlProcess(processInfo):
			processType = processInfo['processType']
			processIndex = processInfo['processIndex']
			self._resourceHandler.putMlProcess(processType, processIndex)

		while True:
			try:
				outputMessage = self._stubResultQ.get()
				self._logger.debug(outputMessage)

				if type(outputMessage) == str or not outputMessage.has_key('stubId'):
					self._logger.warn("# This message not inclueded a stub id")
					self._logger.warn(" - %s" %(outputMessage))
					continue
					
				putMlProcess(outputMessage['processInfo'])
				stubId = outputMessage['stubId']
				socketObj = self._stubMappingTable[stubId]
				DataSender(self._logger, socketObj).sendMsg(outputMessage)
				self._socketClose(stubId)
			except Exception, e:
				self._logger.exception(e)

	def _socketClose(self, stubId):
		socketObj = self._stubMappingTable[stubId]
		if socketObj:
			socketObj.close()
		if self._stubMappingTable.has_key(stubId):
			del self._stubMappingTable[stubId]
