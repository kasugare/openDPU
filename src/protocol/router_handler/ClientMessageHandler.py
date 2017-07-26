#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tcp_modules.NetworkHandler import DataSender
from protocol.message_pool.MessageGenerator import genReqJobCompleted, genResOK, genReqError
from protocol.router_handler.client_protocol.ClientRequestHandler import ClientRequestHandler
from protocol.router_handler.client_protocol.ClientJobResultHandler import ClientJobResultHandler
from nilm_master.job_container.nilm_output_checker.NilmOutputChecker import NilmOutputChecker

class ClientMessageHandler:
	def __init__(self, logger, socketObj, resourceManager, jobQ):
		self._logger = logger
		self._sender = DataSender(logger, socketObj)
		self._clientRequestHandler = ClientRequestHandler(logger, socketObj, resourceManager, jobQ)
		self._clientJobResultHandler = ClientJobResultHandler(logger, socketObj, resourceManager)

	def routeRequestJob(self, message):
		try:
			protocol = message['proto']
			if protocol == 'REQ_NILM_JOB':
				self._clientRequestHandler.doNilmJobOption(message)
			elif protocol == 'REQ_VERSION':
				self._clientRequestHandler.checkClusterVersion(message)
			elif protocol == 'REQ_WORKERS_RESOURCES':
				self._clientRequestHandler.getWorkerResources(message)
			elif protocol == 'REQ_GROUP_INFO':
				self._clientRequestHandler.getNilmGroupInfo(message)
			elif protocol == 'REQ_OUTPUT_CHECK':
				self._clientRequestHandler.checkNilmPredictOutput(message)
			elif protocol == 'REQ_HB':
				pass
			else:
				self._sendMessage(genReqJobCompleted())
		except Exception, e:
			self._logger.exception(e)
			self._sendMessage(genReqError(e))

	def routeResponsJob(self, message):
		try:
			protocol == message['proto']
			if protocol == 'RES_NILM_ML_JOB':
				self._clientJobResultHandler.doNilmMlResultJob(message)
		except Exception, e:
			self._logger.exception(e)
			self._sendMessage(genReqError(e))
			

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
