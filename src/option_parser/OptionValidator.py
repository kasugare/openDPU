#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_datetime import getHourlyDatetime, getDailyDatetime, getPeriodTS
from common.conf_collector import getAllowedProtocol
from common.conf_system import getNilmGroupNames, getSystemEnv, getMaxProcess
from data_store.DataServiceManager import DataServiceManager
# from ecommon.ELogger import ELogger
# from common.Logger import Logger
import traceback
import sys
import os


class OptionValidator:
	def __init__(self, logger, options, args):
		self._logger = logger
		self._userOptions = self._setUserOptions(options, args)
		self._serviceManager = DataServiceManager(self._logger)

		if self._userOptions['debugMode']:
			# self._logger.setLevel(ELogger.DEBUG)
			self._logger.setLevel("DEBUG")
		else:
			# self._logger.setLevel(ELogger.INFO)
			self._logger.setLevel("INFO")


	def _setUserOptions(self, options, args):
		user_options = {}
		if options.sids != None:
			user_options['sids'] = options.sids.split(',')
		if options.job != None:
			user_options['job'] = options.job
		if options.period != None:
			user_options['period'] = options.period
		if options.deviceProtocol != None:
			user_options['deviceProtocol'] = options.deviceProtocol.split(',')
		if options.groups != None:
			user_options['groups'] = options.groups.split(',')
		if options.fileSave != None:
			user_options['fileSave'] = options.fileSave
		if options.injection != None:
			user_options['injection'] = options.injection
		if options.remove != None:
			user_options['remove'] = options.remove.split(',')
		if options.numOfProcess != None:
			user_options['numOfProcess'] = options.numOfProcess
		if options.hasFile != None:
			user_options['hasFile'] = options.hasFile
		if options.onlyRawdata != None:
			user_options['onlyRawdata'] = options.onlyRawdata
		if options.debugMode != None:
			user_options['debugMode'] = options.debugMode
		if options.usagePeriod != None:
			user_options['usagePeriod'] = options.usagePeriod
		if options.deviceMeta != None:
			user_options['deviceMeta'] = options.deviceMeta
		return user_options

	def doCheckOptions(self):
		userOptions = {}
		userOptions['debugMode'] = self._checkLogMode()
		userOptions['sids'] = self._checkSiteId()
		userOptions['dids'] = ['*'] #self._checkDeviceId()
		userOptions['job'] = self._checkJob()
		startTS, endTS, userInputPeriod = self._checkPeriod()
		userOptions['startTS'] = startTS
		userOptions['endTS'] = endTS
		userOptions['userInputPeriod'] = userInputPeriod
		userOptions['deviceProtocol'] = self._checkDeviceProtocol(userOptions)
		userOptions['fileSave'] = self._checkFileSave()
		userOptions['numOfProcess'] = self._checkNumOfProcess()
		userOptions['injection'] = self._checkInjection()
		userOptions['remove'] = self._checkRemove()
		userOptions['hasFile'] = self._checkHasFile()
		userOptions['onlyRawdata'] = self._checkCollectingRawDataMode(userOptions)
		userOptions['usagePeriod'] = self._checkUsagePeriod()
		userOptions['deviceMeta'] = self._checkDeviceMeta()
		return userOptions

	def _checkSiteId(self):
		tarSids = []
		sids = self._userOptions.get('sids')
		def isNumber(string):
			try:
				int(string)
				return True
			except ValueError:
				return False

		for sid in sids:
			if isNumber(sid):
				pass
			else:
				if sid == '*':
					groupNames = self._userOptions.get('groups')
					if not groupNames:
						groupNames = getNilmGroupNames()
					sids = self._serviceManager.getNilmTargetSids(groupNames)
					return sids
				else:
					self._logger.warn("# Wrong site ids in the site id. Device id will be integer number or '*'.")
					sys.exit(1)

		return [int(sid) for sid in sids]

	def _checkDeviceId(self):
		tarDids = []
		dids = self._userOptions.get('dids')

		def isNumber(string):
			try:
				int(string)
				return True
			except ValueError:
				return False

		for did in dids:
			if isNumber(did):
				return [did]
			else:
				if did == '*':
					return [did]
				else:
					self._logger.warn("# Wrong device ids in the site id. Device id will be integer number or '*'.")
					sys.exit(1)

	def _checkPeriod(self):
		def _checkUsagePeriod(period):
			if period == 'hourly':
				startTS, endTS = getHourlyDatetime()
				return startTS, endTS
			elif period == 'daily':
				startTS, endTS = getDailyDatetime()
				return startTS, endTS
			else:
				return None, None

		def _checkUserPeriod(period):
			if self._userOptions.get('remove'):
				return 0, 0
			if not period or not len(period) == 2:
				return None, None
			else:
				startDT = period[0]
				endDT = period[1]
				startTS, endTS = getPeriodTS(startDT, endDT)
				return startTS, endTS

		def _error():
			self._logger.warn("# Wrong timestamp format. check your time range formats.")
			sys.exit(1)

		period = self._userOptions.get('period')
		if period:
			startTS, endTS = _checkUserPeriod(period)
			userInputPeriod = True
		else:
			period = self._userOptions.get('usagePeriod')
			startTS, endTS = _checkUsagePeriod(period)
			userInputPeriod = False

		if not startTS or not endTS:
			_error()

		if startTS > endTS:
			_error()
		return startTS, endTS, userInputPeriod

	def _checkUsagePeriod(self):
		usagePeriod = self._userOptions.get('usagePeriod')
		if usagePeriod == 'hourly' or usagePeriod == 'daily':
			return usagePeriod
		else:
			self._logger.warn("""# Wrong usage period. (You have to use one such as 'hourly' or 'daily'""")
			sys.exit(1)

	def _checkDeviceProtocol(self, tempUserOptions):
		deviceProtocols = []
		ALLOWED_PROTOCOLS = getAllowedProtocol()
		inputProtocols = self._userOptions.get('deviceProtocol')
		# try:
		# 	sid = int(tempUserOptions['sids'][0])
		# 	instType = self._serviceManager.getEdmType(sid)
		# 	envMode = getSystemEnv()
		# 	if instType != None:
		# 		for deviceProtocol in ALLOWED_PROTOCOLS:
		# 			if instType.lower() == deviceProtocol.split('_')[0]:
		# 				if len(deviceProtocol.split('_')) == 3 and envMode == 'staging':
		# 					return [deviceProtocol]
		# 				elif len(deviceProtocol.split('_')) == 2 and envMode == 'production':
		# 					return [deviceProtocol]
		# 	else:
		# 		self._logger.warn("wrong site id : %d" %sid)
		# 		sys.exit(1)
		# except Exception, e:
		# 	self._logger.exception(e)
		if inputProtocols:
			for inputProtocol in inputProtocols:
				if inputProtocol not in ALLOWED_PROTOCOLS:
					self._logger.warn("# %s is not in allowed protocols.' %dProtocol")
					sys.exit(1)
				else:
					if inputProtocol == '*':
						deviceProtocols = ['*']
						return deviceProtocols
					else:
						deviceProtocols.append(inputProtocol)
				deviceProtocols = list(set(deviceProtocols))
		else:
			deviceProtocols = ALLOWED_PROTOCOLS
		return deviceProtocols

	def _checkJob(self):
		job = self._userOptions.get('job')
		if job == 'app_search' or job == 'app_usage' or job == 'candidate':
			return job
		elif not job:
			return None
		else:
			self._logger.warn("""# Wrong job request. (You have to use one such as 'app_search', 'app_usage' , 'candidate' or None)""")
			sys.exit(1)

	def _checkFileSave(self):
		return self._userOptions.get('fileSave')

	def _checkInjection(self):
		return self._userOptions.get('injection')

	def _checkNumOfProcess(self):
		numOfProcess = self._userOptions.get('numOfProcess')
		maxNumOfProcess = getMaxProcess()
		if numOfProcess > maxNumOfProcess:
			numOfProcess = maxNumOfProcess
		return numOfProcess

	def _checkRemove(self):
		REMOVE_OPTIONS = ['appliance', 'meta', 'router', 'vfeeder', 'all', '*']
		options = self._userOptions.get('remove')

		if not options:
			return None

		options = list(set(options))
		if 'vfeeder' in options or 'all' in options or '*' in options:
			return ['vfeeder']

		sids = self._checkSiteId()
		if '*' in sids or len(sids) == 0 or len(sids) >1:
			self._logger.warn("# Wrong remove options. You have to use -s/--sids options.")
			sys.exit(1)

		for option in options:
			if not option in REMOVE_OPTIONS:
				self._logger.warn("# Wrong remove options.")
				sys.exit(1)
		return options

	def _checkHasFile(self):
		return self._userOptions.get('hasFile')

	def _checkCollectingRawDataMode(self, userOptions):
		isOnlyRawData = self._userOptions.get('onlyRawdata')
		if isOnlyRawData:
			sids = [int(sid) for sid in self._userOptions.get('sids')]
			userOptions['sids'] = sids
			return isOnlyRawData
		return isOnlyRawData

		# return self._userOptions.get('onlyRawdata')

	def _checkLogMode(self):
		return self._userOptions.get('debugMode')

	def _checkDeviceMeta(self):
		return self._userOptions.get('deviceMeta')
