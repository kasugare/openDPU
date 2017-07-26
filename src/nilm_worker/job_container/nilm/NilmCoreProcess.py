#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_nilm_core import getNilmCorePath
from common.conf_system import getDebugDataStoreInfo
from common.util_common import convertDictToString
from fs_handler.FSHandler import FSHandler
from threading import Thread, Lock
import subprocess
import signal
import shutil
import time
import json
import sys
import os

class NilmCoreProcess:
	def __init__(self, logger, rLogger, debugMode=False):
		self._logger = logger
		self._rLogger = rLogger
		self._rProcess = None
		self._debugMode = debugMode
		self._sid = None
		self._nilmSiteInfo = None
		self._nilmCorePath = getNilmCorePath()
		self._fsHandler = FSHandler(logger)

	def __del__(self):
		if self._rProcess:
			self._rProcess.terminate()
			# os.killpg(os.getpgid(self._rProcess.pid), signal.SIGTERM)

	def setSidInfo(self, sid, nilmSiteInfo):
		self._sid = sid
		self._nilmSiteInfo = os.path.basename(nilmSiteInfo)

	def runLearnAppMetas(self, siteInfoFilePath, nilmRawDataFilePath):
		self._logger.info("#[Learn] Do analyze NILM meta detection")
		jobType='learning'
		appMetas = None

		# New command
		cmd = "%s/run.R -a CyclicLoad,HeavyLoad,RiceCooker,StandByPower,Tv,Washer,AirConditioner -o learn -s %s -t -l DEBUG %s" %(self._nilmCorePath, siteInfoFilePath, nilmRawDataFilePath)
		self._saveDebuggingData(jobType=jobType, rCmd=cmd, siteInfoFilePath=siteInfoFilePath, nilmRawDataFilePath=nilmRawDataFilePath)

		try:
			self._logger.debug("- R command : %s" %cmd)
			self._rProcess = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			self._runLogger()
			rawMetas = self._rProcess.stdout.read()
			appMetas = convertDictToString(json.loads(rawMetas))
		except Exception, e:
			self._logger.exception(e)
		finally:
			self._rProcess.terminate()

		self._saveDebuggingData(jobType=jobType, result=str(appMetas))
		return appMetas

	def runUpdateAppMetas(self, metaPath, siteInfoFilePath, nilmRawDataFilePath):
		self._logger.info("#[Learn] Do analyze NILM meta detection")
		jobType='learning_update'
		appMetas = None

		# New command
		cmd = "%s/run.R -a CyclicLoad,HeavyLoad,RiceCooker,StandByPower,Tv,Washer,AirConditioner -o learn -m %s -s %s -t -l DEBUG %s" %(self._nilmCorePath, metaPath, siteInfoFilePath, nilmRawDataFilePath)
		self._saveDebuggingData(jobType=jobType, rCmd=cmd, metaFilePath=metaPath, siteInfoFilePath=siteInfoFilePath, nilmRawDataFilePath=nilmRawDataFilePath)

		try:
			self._logger.debug("- R command : %s" %cmd)
			self._rProcess = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			self._runLogger()
			rawMetas = self._rProcess.stdout.read()
			appMetas = convertDictToString(json.loads(rawMetas))
		except Exception, e:
			self._logger.exception(e)
		finally:
			self._rProcess.terminate()

		self._saveDebuggingData(jobType=jobType, result=str(appMetas))
		return appMetas

	def runUsagePrediction(self, metaPath, siteInfoFilePath, nilmRawDataFilePath, rExtOpt):
		self._logger.info("# [Prediction] Do analyze NILM daily usage")
		jobType='predict'
		predictResult = None

		# New command
		if not rExtOpt:
			cmd = "%s/run.R -a CyclicLoad,HeavyLoad,RiceCooker,StandByPower,Tv,Washer,AirConditioner -o predict -m %s -s %s -t -l DEBUG %s" %(self._nilmCorePath, metaPath, siteInfoFilePath, nilmRawDataFilePath)
		else:
			cmd = "%s/run.R -a CyclicLoad,HeavyLoad,RiceCooker,StandByPower,Tv,Washer,AirConditioner -o predict -m %s -s %s -t -l DEBUG %s -e %s" %(self._nilmCorePath, metaPath, siteInfoFilePath, nilmRawDataFilePath, rExtOpt)
		
		self._saveDebuggingData(jobType=jobType, rCmd=cmd, metaFilePath=metaPath, siteInfoFilePath=siteInfoFilePath, nilmRawDataFilePath=nilmRawDataFilePath)

		try:
			self._logger.debug("- R command : %s" %cmd)
			self._rProcess = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			self._runLogger()
			self._rProcess.stderr.readline()
			predictUsage = self._rProcess.stdout.read()
			if predictUsage:
				predictUsage = predictUsage.replace('''"NA"''', '0')
				predictResult = convertDictToString(json.loads(predictUsage))
		except Exception, e:
			self._logger.exception(e)
			self._logger.error("Unexpected error: %s" %str(sys.exc_info()[0]))
		finally:
			self._rProcess.terminate()

		self._saveDebuggingData(jobType=jobType, result=str(predictResult))
		return predictResult

	def _runLogger(self):
		loggerThread = Thread(target=self._rLoggerThread, args=())
		loggerThread.setDaemon(1)
		loggerThread.start()

	def _rLoggerThread(self):
		reload(sys)
		sys.setdefaultencoding('utf-8')

		def _infoLog(message):
			self._rLogger.info("[%d] %s" %(self._sid, str(unicode(message))))
		def _debugLog(message):
			self._rLogger.debug("[%d] %s" %(self._sid, str(unicode(message))))
		def _warnLog(message):
			self._rLogger.warn("[%d] %s ( REF : %s )" %(self._sid, str(unicode(message)), self._nilmSiteInfo))
		def _errorLog(message):
			self._rLogger.error("[%d] %s ( REF : %s )" %(self._sid, str(unicode(message)), self._nilmSiteInfo))
		def _fatalLog(message):
			self._rLogger.fatal("[%d] %s ( REF : %s )" %(self._sid, str(unicode(message)), self._nilmSiteInfo))
		def _criticalLog(message):
			self._rLogger.critical("[%d] %s ( REF : %s )" %(self._sid, str(unicode(message)), self._nilmSiteInfo))

		while True:
			rLog = self._rProcess.stderr.readline().replace('\n','')
			if not rLog:
				break
			splitedLog = rLog.split(']')

			if len(splitedLog) < 2:
				_infoLog(rLog)
			else:
				logLevel = splitedLog[0].replace('[','')
				message = "]".join(splitedLog[2:])
				if logLevel == 'INFO':
					_infoLog(message)
				elif logLevel == 'DEBUG':
					_debugLog(message)
				elif logLevel == 'WARING':
					_warnLog(message)
				elif logLevel == 'ERROR':
					_errorLog(message)
				elif logLevel == 'FATAL':
					_fatalLog(message)
				elif logLevel == 'CRITICAL':
					_criticalLog(message)
				else:
					_infoLog(message)

	def _saveDebuggingData(self, jobType='debug', rCmd=None, metaFilePath=None, siteInfoFilePath=None, nilmRawDataFilePath=None, result=None):
		if not self._debugMode:
			return
		self._logger.debug("# Activated debuging mode for Nilm-Core")

		debugDataPath = getDebugDataStoreInfo()
		if not os.path.exists(debugDataPath):
			self._fsHandler.mkdirOnLocal(debugDataPath)

		debuggingSitePath = os.path.join(debugDataPath, "%s_%d" %(jobType, self._sid))
		if not os.path.exists(debuggingSitePath):
			debuggingSitePath = self._fsHandler.mkdirOnLocal(debuggingSitePath)

		if rCmd:
			cmdFileName = 'r_command_%d_%d.sh' %(self._sid, int(time.time()*1000))
			cmdFilePath = os.path.join(debuggingSitePath, cmdFileName)
			fd = open(cmdFilePath, 'w')
			try:
				fd.write('%s\n' %(rCmd))
				fd.close()
			except Exception, e:
				self._logger.exception(e)
				if fd:
					fd.close()

		if metaFilePath:
			try:
				shutil.copy(metaFilePath, debuggingSitePath)
			except Exception, e:
				self._logger.exception(e)

		if siteInfoFilePath:
			try:
				shutil.copy(siteInfoFilePath, debuggingSitePath)
			except Exception, e:
				self._logger.exception(e)

		if nilmRawDataFilePath:
			try:
				shutil.copy(nilmRawDataFilePath, debuggingSitePath)
			except Exception, e:
				self._logger.exception(e)

		if result:
			resultFileName = 'r_result_%d_%d' %(self._sid, int(time.time()*1000))
			resultFilePath = os.path.join(debuggingSitePath, resultFileName)
			fd = open(resultFilePath, 'w')
			try:
				fd.write('%s\n' %(rCmd))
				fd.close()
			except Exception, e:
				self._logger.exception(e)
				if fd:
					fd.close()


