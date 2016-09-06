#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from common.util_protocol_parser import parseProtocol
from message_pool.cluster_protocol_messages import genGetWidMsg, genGetWorkerResources
from message_pool.system_protocol_messages import getSetWorkerResource, genDelWorkerResource, genConnectionLost, genDelWorkerResource
from dpu_master.network_manager.MasterConnectionServer import MasterConnectionServer
from dpu_master.network_manager.HealthChecker import HealthChecker
from threading import Thread, Lock
import multiprocessing
import time

PROCESS_NAME = 'MASTER_NETWORK'

class MasterNetworkManager:
	def __init__(self, resourceQ):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('# process start : %s' %PROCESS_NAME)
		self._reqQ = resourceQ['reqQ']
		self._routerQ = resourceQ['routerQ']
		self._networkQ = multiprocessing.Queue()
		self._tempProviders = {}
		self._providers = {}

	def __del__(self):
		self._logger.warn('@ terminate process : %s' %PROCESS_NAME)
		for tempId in self._tempProviders.keys():
			self._tempProviders[tempId].close()
		for workerId in self._providers.keys():
			self._providers[workerId].close()

	def _sendMsg(self, **args):
		networkObj = None
		try:
			if args.has_key('tempId'):
				networkObj = self._tempProviders[args['tempId']]
			elif args.has_key('workerId'):
				networkObj = self._providers[args['workerId']]
		except KeyError, e:
			self._logger.warn('# key error : %s' %(str(args)) )

		if not networkObj:
			self._logger.warn('# network object not exist.')
			return False
		return networkObj.sendMsg(args)

	def _delProvider(self, **args):
		if args.has_key('tempId'):
			tempId = args['tempId']
			if self._tempProviders.has_key(tempId):
				del self._tempProviders[tempId]
		if args.has_key('workerId'):
			workerId = args['workerId']
			if self._providers.has_key(workerId):
				del self._providers[workerId]

	def _closeProvider(self, **args):
		if args.has_key('tempId'):
			tempId = args['tempId']
			if self._tempProviders.has_key(tempId):
				networkObj = self._tempProviders[tempId]
				if networkObj:
					networkObj.close()
		if args.has_key('workerId'):
			workerId = args['workerId']
			if self._providers.has_key(workerId):
				networkObj = self._providers[workerId]
				if networkObj:
					networkObj.close()
		self._delProvider(**args)

	def doProcess(self):
		try:
			clusterQHandler = Thread(target=self._runClusterRequestHandler, args=())
			clusterQHandler.setDaemon(1)
			clusterQHandler.start()

			MasterConnectionServer(self._logger, self._networkQ, self._tempProviders).start()
			self._runSystemRequestHandler()

		except KeyboardInterrupt, e:
			pass

	def _runSystemRequestHandler(self):
		while True:
			reqMsg = self._reqQ.get()
			print "[Q] MasterNetworkManager(SYS) :", reqMsg
			protocol, statCode = parseProtocol(reqMsg)

			if protocol == 'SYS_CONN_CLOSE':
				workerId = reqMsg['workerId']
				self._routerQ.put_nowait(genDelWorkerResource(workerId))
				self._closeProvider(workerId = workerId)

	def _runClusterRequestHandler(self):
		while True:
			print "-" * 100
			print self._tempProviders
			print "-" * 100
			print self._providers
			print "-" * 100

			try:
				reqMsg = self._networkQ.get()
				print "[Q] MasterNetworkManager(CLUSTER) :", reqMsg
				protocol, statCode = parseProtocol(reqMsg)

				if protocol == 'MW_NET_GET_WID':
					self._sendMsg(**reqMsg)
				elif protocol == 'WM_NET_SET_WID':
					if statCode == 200:
						tempId = reqMsg['result']['tempId']
						workerId = reqMsg['result']['workerId']
						try:
							if self._providers.has_key(workerId):
								self._logger.warn("# conflict worker id : %s" %workerId)
								self._closeProvider(workerId = workerId, tempId = tempId)
								# self._reqQ.put_nowait(genConnectionLost(workerId))
							else:
								networkObj = self._tempProviders[tempId]
								networkObj.setWorkerId(workerId)
								self._providers[workerId] = networkObj
								self._delProvider(tempId = tempId)
								HealthChecker(self._logger, self._reqQ, workerId, networkObj).start()
								self._networkQ.put_nowait(genGetWorkerResources(workerId))
						except EOFError, e:
							self._logger.error("EOFError")
							self._closeProvider(tempId = tempId, workerId = workerId)
						except Exception, e:
							self._logger.exception(e)
							self._closeProvider(tempId = tempId, workerId = workerId)
					else:
						self._closeProvider(tempId = tempId, workerId = workerId)
				elif protocol == 'MW_NET_STAT_HB':
					self._sendMsg(**reqMsg)
				elif protocol == 'SYS_CONN_CLOSE':
					workerId = reqMsg['workerId']
					self._routerQ.put_nowait(genDelWorkerResource(workerId))
					self._closeProvider(workerId = workerId)
				elif protocol == 'MW_RS_GET_RESOURCE':
					self._sendMsg(**reqMsg)
				elif protocol == 'WM_RS_SET_RESOURCE':
					self._routerQ.put_nowait(getSetWorkerResource(reqMsg['workerId'], reqMsg['result']))

			except ValueError, e:
				self._logger.exception(e)
