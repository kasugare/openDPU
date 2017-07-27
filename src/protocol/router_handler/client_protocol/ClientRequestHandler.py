#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_version import getVersionInfo
from common.util_common import cvtWorkerId as cvtClientId
from protocol.message_pool.MessageGenerator import genReqJobCompleted, genResOK, genReqError, genReqDpuRawData, genResVersion, genResDpuGroupInfo, genResWorkersResources, genResDpuOutputCheckInfo
from tcp_modules.NetworkHandler import DataSender
import pickle
import copy
import sys

class ClientRequestHandler:
	def __init__(self, logger, socketObj, resourceManager, jobQ):
		self._logger = logger
		self._jobQ = jobQ
		self._resourceManager = resourceManager
		self._clientId = cvtClientId(socketObj)
		self._sender = DataSender(logger, socketObj)

	def checkClusterVersion(self, message):
		orderSheet = message['orderSheet']
		if orderSheet.has_key('version'):
			versionInfo = getVersionInfo()
			self._sendMessage(genResVersion(versionInfo))

	def getWorkerResources(self, message):
		orderSheet = message['orderSheet']
		if not orderSheet.has_key('workers'):
			self._sendMessage(genReqError("# Workers are not activate"))
			self._sendMessage(genReqError(e))
			return

		workers = orderSheet['workers']
		if 'all' in workers:
			try:
				workerResourcesMessage = self._getWorkerResoures()
				self._sendMessage(genResWorkersResources(workerResourcesMessage))
			except Exception, e:
				self._logger.exception(e)
				self._sendMessage(genReqError(e))

	def _getWorkerResoures(self):
		workerResources = copy.deepcopy(self._resourceManager.getWorkerResoures())
		priorityWorkerPool = self._resourceManager.getPriorityWorkerPool()
		privateWorkerPool = self._resourceManager.getPrivateWorkerPool()
		publicWorkerPool = self._resourceManager.getPublicWorkerPool()
		priorityJobPool = self._resourceManager.getPriorityJobPool()
		privateJobPool = self._resourceManager.getPrivateJobPool()
		publicJobPool = self._resourceManager.getPublicJobPool()

		priorityStack = len(self._resourceManager.getPrevWorkerObjs())
		priorityEnabled = self._resourceManager.isTajoEnabled()

		priorityWorkerSize = priorityWorkerPool.qsize()
		privateWorkerSize = privateWorkerPool.qsize()
		publicWorkerSize = publicWorkerPool.qsize()
		priorityJobSize = priorityJobPool.qsize()
		privateJobSize = privateJobPool.qsize()
		publicJobSize = publicJobPool.qsize()

		currentPriorityWorkerId = self._resourceManager.getCurrPriorityWorker()
		if currentPriorityWorkerId:
			if workerResources.has_key(currentPriorityWorkerId):
				workerResources[currentPriorityWorkerId]['priority_runner'] = True
		else:
			for workerId in workerResources.keys():
				workerResources[workerId]['priority_runner'] = False

		workerResourcesMessage = {
			'workers': {
				'priority': priorityWorkerSize,
				'private': privateWorkerSize,
				'public': publicWorkerSize,
				'priority_enabled': str(priorityEnabled)
			},
			'jobs': {
				'priority': priorityJobSize,
				'private': privateJobSize,
				'public': publicJobSize,
				'priority_stack': priorityStack
			},
			'resources': workerResources
		}
		return workerResourcesMessage

	def _sendMessage(self, message):
		try:
			self._sender.sendMsg(message)
		except Exception, e:
			self._logger.exception(e)

	def _closeConnection(self):
		try:
			self._sender._socketObj.close()
		except Exception, e:
			self._logger.exception(e)
