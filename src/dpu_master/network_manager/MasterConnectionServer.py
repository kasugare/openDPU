#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_network import getHostInfo
from network.NetworkHandler import NetworkHandler
from message_pool.cluster_protocol_messages import genGetWidMsg
from dpu_master.network_manager.HealthChecker import HealthChecker
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread, Lock
import random
import time

class MasterConnectionServer(Thread):
	def __init__(self, logger, networkQ, tempProviders):
		Thread.__init__(self)
		self._logger = logger
		self._networkQ = networkQ
		self._tempProviders = tempProviders

	def run(self):
		try:
			hostIp, hostPort = getHostInfo('MASTER')
			svrsock = socket(AF_INET, SOCK_STREAM)
			svrsock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
			svrsock.bind((hostIp, hostPort))
			svrsock.listen(5)
			while True:
				socketObj, addr = svrsock.accept()
				networkObj = NetworkHandler(self._logger, socketObj)
				networkObj.runMsgToQueue(self._networkQ)

				tempId = "temp_%d%X" %(random.randrange(0,9), int(time.time()*100000))
				self._tempProviders[tempId] = networkObj
				time.sleep(0.2)
				self._networkQ.put_nowait(genGetWidMsg(tempId))
				self._logger.info("- connected resource provider, addr : %s, port : %d" %(addr[0], addr[1]))
		except KeyboardInterrupt, e:
			pass
