#!/usr/bin/env python
# -*- coding: utf-8 -*-

from protocol.message_pool.MessageGenerator import genResHB, genReqConnClose, genResOK
from common.conf_collector import getDataCollector
from common.conf_network import getHostInfo
from nilm_master.collection_manager.CollectionMessageGenerator import CollectionMessageGenerator
from tcp_modules.NetworkHandler import DataReceiver, DataSender
from ProtocolAnalyzer import ProtocolAnalyzer
from socket import socket, AF_INET, SOCK_STREAM
import time

class CollectorsMsgHandler(ProtocolAnalyzer):
	def __init__(self, logger, resourceManager):
		ProtocolAnalyzer.__init__(self, logger)
		self._resourceManager = resourceManager
		self._messageGenerator = CollectionMessageGenerator(logger)
		self._logger = logger

	def routeProtocol(self, message):
		proto = self.analyzeMessage(message)

		try:
			if proto == 'REQ_NILM':
				protoMsgs = self._messageGenerator.genCollectionMessages(message['params'])
				for protoMsg in protoMsgs:
					self._connCollectorServer(protoMsg)
			elif proto == 'RES_NILM_DATA':
				self._connJobManagerServer(message)
		except Exception, e:
			self._logger.exception(e)
			self._logger.error("# Data collector server not ready.")

	def _connJobManagerServer(self, message):
		hostIp, hostPort = getHostInfo('JOB_MANAGER')
		socketObj = socket(AF_INET, SOCK_STREAM)
		socketObj.connect((hostIp, hostPort))
		sender = DataSender(self._logger, socketObj)
		try:
			sender.sendMsg(message)
		except Exception, e:
			self._logger.exception(e)

	def _connCollectorServer(self, protoMsg):
		socketObj = socket(AF_INET, SOCK_STREAM)
		if protoMsg:
			if protoMsg['params']['analysisType'] == 'candidate':
				socketObj.connect((getDataCollector('CANDIDATE_COLLECTOR')))
			else:
				socketObj.connect((getDataCollector()))

		sender = DataSender(self._logger, socketObj)
		receiver = DataReceiver(self._logger, socketObj)
		sender.sendMsg(protoMsg)

		while True:
			recvMessage = receiver.recvMsg()
			try:
				protocol = recvMessage['proto']
				if not recvMessage:
					break
				if protocol == 'REQ_HB':
					sender.sendMsg(genResHB())
				elif protocol == 'REQ_JC':
					sender.sendMsg(genReqConnClose())
					break
				elif protocol == 'RES_NILM_DATA':
					self.routeProtocol(recvMessage)
				elif protocol == 'REQ_PROGS':
					self._logger.info(str(recvMessage['message']))
			except Exception, e:
				self._logger.exception(e)
				break
		try:
			if socketObj:
				self._logger.info('## Socket Close')
				socketObj.close()
		except Exception, e:
			self._logger.warn("# Socket was already closed!")
			self._logger.exception(e)
