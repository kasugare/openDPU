#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_system import getHeartBeatIntervalTime, getMaxRetryCount
from message_pool.cluster_protocol_messages import genReqHeartBeat
from message_pool.system_protocol_messages import genConnectionLost
import threading
import time

class HealthChecker(threading.Thread):
	def __init__(self, logger, systemQ, workerId, networkObj):
		threading.Thread.__init__(self)
		self._logger = logger
		self._systemQ = systemQ
		self._workerId = workerId
		self._networkObj = networkObj
		self._maxRetry = getMaxRetryCount()
		self._intervalTime = getHeartBeatIntervalTime()

	def __del__(self):
		# self._systemQ.put_nowait(genConnectionLost(self._workerId, 'worker connection closed'))
		pass

	def run(self):
		try:
			while True:
				if self._networkObj.isAlive():
					self._networkObj.sendMsg(genReqHeartBeat(self._workerId))
				time.sleep(getHeartBeatIntervalTime())
		except EOFError, e:
			pass
		except Exception, e:
			self._logger.exception(e)