#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from common.conf_network import getHostInfo
from common.util_protocol_parser import parseProtocol
from message_pool.system_protocol_messages import genGetWorkerResource, genGetSysWidMsg
from message_pool.cluster_protocol_messages import genSetWid, genResHeartBeat, genSetWorkerResource
from network.NetworkHandler import NetworkHandler
from threading import Thread, Lock
import multiprocessing
import time


PROCESS_NAME = 'WORKER_NETWORK'

class WorkerNetworkManager:
	def __init__(self, workerId, networkQ):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('# process start : %s' %PROCESS_NAME)
		self._reqQ = networkQ['reqQ']
		self._routerQ = networkQ['routerQ']
		self._networkQ = multiprocessing.Queue()
		self._workerId = workerId
		self._networkObj = None


	def __del__(self):
		self._logger.warn('@ terminate process : %s' %PROCESS_NAME)

	def doProcess(self):
		try:
			connGatewayQHandler = Thread(target=self._runConnectionGateway, args=())
			connGatewayQHandler.setDaemon(1)
			connGatewayQHandler.start()

			self._connectMasterServer()

			while True:
				reqMsg = self._reqQ.get()
				print "[Q] WorkerNetworkManager(SYS) :", reqMsg
				protocol, statCode = parseProtocol(reqMsg)

				if protocol == 'SYS_GET_WID':
					tempId = reqMsg['tempId']
					self._workerId = reqMsg['workerId']
					self._networkQ.put_nowait(genSetWid(reqMsg['tempId'], reqMsg['workerId']))
				elif protocol == 'SYS_RES_RESOURCE':
					self._networkQ.put_nowait(genSetWorkerResource(reqMsg['workerId'], reqMsg['result']))
		except KeyboardInterrupt, e:
			pass

	def _connectMasterServer(self):
		while True:
			networkObj = NetworkHandler(self._logger)
			networkObj.connection(getHostInfo('MASTER'))
			networkObj.runMsgToQueue(self._networkQ)
			if self._workerId:
				networkObj.setWorkerId(self._workerId)
			if networkObj.isAlive():
				self._logger.info("# connected resource server of DPU")
				self._networkObj = networkObj
				return
			else:
				self._logger.warn("# DPU server not ready : %s" %(self._workerId))
				time.sleep(1)

	def _runConnectionGateway(self):
		while True:
			try:
				reqMsg = self._networkQ.get()
				print "[Q] WorkerNetworkManager(CLUSTER) :", reqMsg
				protocol, statCode = parseProtocol(reqMsg)

				if protocol == 'MW_NET_GET_WID':
					tempId = reqMsg['tempId']
					self._routerQ.put_nowait(genGetSysWidMsg(tempId))
					# self._networkObj.sendMsg(genSetWid(tempId, self._workerId))
				elif protocol == 'WM_NET_SET_WID':
					self._networkObj.sendMsg(reqMsg)
					if not self._networkObj.hasWorkerId():
						self._networkObj.setWorkerId(self._workerId)
				elif protocol == 'SYS_CONN_CLOSE':
					self._logger.error("# server connection closed.")
					self._connectMasterServer()
				elif protocol == 'MW_NET_STAT_HB':
					self._networkObj.sendMsg(genResHeartBeat(self._workerId))
				elif protocol == 'MW_RS_GET_RESOURCE':
					workerId = reqMsg['workerId']
					self._routerQ.put_nowait(genGetWorkerResource(workerId))
				elif protocol == 'WM_RS_SET_RESOURCE':
					self._networkObj.sendMsg(reqMsg)


					# resource = self.getCurrentResource()

					# self._networkObj.sendMsg(genHBCheckOkMsg(workerId, resource))
			except EOFError, e:
				pass
