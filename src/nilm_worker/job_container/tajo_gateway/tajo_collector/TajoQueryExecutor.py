#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_system import getHomeDir
from common.conf_collector import getTajoQueryFailorverOptions
from protocol.message_pool.MessageGenerator import genReqProgressMessage, genReqTajoEnable
import subprocess
import time

class TajoQueryExecutor:
	def __init__(self, logger, sendMessageQ, jobType):
		self._logger = logger
		self._sendMessageQ = sendMessageQ
		self._jobType = jobType
		
	def doExecuteQuery(self, queryPathMap):
		tempFilePathList = []
		retryCount, sleepTime = getTajoQueryFailorverOptions()
		for tempFilePath in queryPathMap.keys():
			for queryString in queryPathMap[tempFilePath]:
				runningCount = 0
				while runningCount < retryCount:
					if self._doNilmDataCollection(tempFilePath, queryString):
						tempFilePathList.append(tempFilePath)
						break
					else:
						runningCount += 1
						time.sleep(sleepTime)
				if runningCount >= retryCount:
					self._sendMessageQ.put_nowait(genReqTajoEnable(self._jobType))
		return tempFilePathList

	def _doNilmDataCollection(self, tempFilePath, queryString):
		self._logger.info('# Do process for nilm data collection')
		command = """%s/src/nilm_worker/job_container/tajo_gateway/resources/dataQuery.sh "%s" | sed '1d' | sed '1d' | head -n -2 > %s""" %(getHomeDir(), queryString, tempFilePath)
		self._logger.info("- command : %s" %command)
		querySucceeded = False
		hasBeenSent = False
		try:
			process = subprocess.Popen(command, shell=True, stdout=None, stderr=subprocess.PIPE)
			self._logger.info("- Create Subprocess, PID : %d" %(process.pid))
			while True:
				progressMessage = process.stderr.readline()

				if not progressMessage:
					self._logger.info("- completed raw data aggregation by tajo")
					break
				header = progressMessage[:1]
				processRate = progressMessage.replace('%', '').split(',')[0].split(' ')[1]
				if header == 'P':
					self._logger.debug(progressMessage.split('\n')[0])
					if processRate == '100' and not querySucceeded:
						querySucceeded = True
						if not hasBeenSent:
							self._sendMessageQ.put_nowait(genReqTajoEnable(self._jobType))
							hasBeenSent = True
				elif header == '-':
					self._logger.info(" - complete analysis raw data and aggregating raw data ...")
		except Exception, e:
			self._logger.warn('# tajo query failed')
			self._logger.exception(e)
			querySucceeded = False
		finally:
			if process:
				self._logger.info("# terminate subprocess, PID : %d" %(process.pid))
				process.terminate()

		self._logger.info("# completed tajo query")
		return querySucceeded
