#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_print import printStatusMap, printMap
from common.util_datetime import getUtcNowTS
from common.conf_country import getCountryDeviceMetaInfos
from nilm_worker.job_container.nilm_status_checker.database.DataServiceManager import DataServiceManager
import copy

class SiteStatusManager:
	def __init__(self, logger):
		self._logger = logger
		self._nilmStatusMap = {}

	def doProcess(self, startTS, endTS, gids):
		dbServiceManager = DataServiceManager(self._logger)

		self._logger.info("# 1. Get nilm site status info on rds_config")
		nilmSiteStatusMap = dbServiceManager.getSiteStatusMap()

		self._logger.info("# 2. Get nilm group sids on rds_config")
		tarNilmSiteMap = dbServiceManager.getTargetNilmSiteMap(gids)

		self._logger.info("# 3. Get existed nilm meta sids on rds_config")
		regMetaSidsMap = dbServiceManager.getRegistratedNilmMetaSids()

		self._logger.info("# 4. Get activated sids on tajo")
		activeDeviceSiteMap = dbServiceManager.getActiveDeviceSiteMap(startTS, endTS)

		self._logger.info("# 5. Combine nilmMaps with each elements")
		currentNilmStatusMap = self._cvtNilmStatusFormat(tarNilmSiteMap, nilmSiteStatusMap, activeDeviceSiteMap, regMetaSidsMap)

		self._logger.info("# 6. Extract the changed status info")
		changedNilmStatusMap = self._extractToUpdateNilmStatus(nilmSiteStatusMap, currentNilmStatusMap)
		sids = changedNilmStatusMap.keys()

		if sids:
			self._logger.info("# 7. Set device common infos")
			siteCountryCodeInfoMap = dbServiceManager.getSiteCountryCodeInfos(sids)
			changedNilmStatusMap = self._setDeviceCountryInfo(siteCountryCodeInfoMap, changedNilmStatusMap)

			self._logger.info("# 8. Insert changedNilmStatusMap into the nilm_site_status table on rds_config")
			dbServiceManager.setNilmStatusQuery(changedNilmStatusMap)

		printMap(self._logger, changedNilmStatusMap)
		self._logger.info("# Done")

	def _cvtNilmStatusFormat(self, tarNilmSiteMap, nilmSiteStatusMap, activeDeviceSiteMap, regMetaSidsMap):
		currentNilmStatusMap = copy.deepcopy(nilmSiteStatusMap)

		self._logger.info(" - 1) combine currentNilmStatusMap with allowed nilm group")
		for sid in tarNilmSiteMap:
			if not sid in currentNilmStatusMap.keys():
				did = tarNilmSiteMap[sid]
				currentNilmStatusMap[sid] = self._getNilmFormat(sid, did, nilmStatus='active')
			else:
				if currentNilmStatusMap[sid]['nilmStatus'] == 'inactive':
					currentNilmStatusMap[sid]['nilmStatus'] = 'active'
					currentNilmStatusMap[sid]['updateTS'] = getUtcNowTS()

		self._logger.info(" - 2) check currentNilmStatusMap whether has meta in regMetaSidsMap or not ")
		for sid in regMetaSidsMap.keys():
			if not sid in currentNilmStatusMap.keys():
				did = regMetaSidsMap[sid].keys()[0]
				regTS = regMetaSidsMap[sid][did]
				currentNilmStatusMap[sid] = self._getNilmFormat(sid, did, regTS=regTS)
			else:
				if currentNilmStatusMap[sid]['hasMeta'] == 0:
					currentNilmStatusMap[sid]['hasMeta'] = 1
					currentNilmStatusMap[sid]['updateTS'] = getUtcNowTS()

		self._logger.info(" - 3) check registrated nilm meta on currentNilmStatusMap")
		notRegMetaSids = list(set(currentNilmStatusMap.keys()).difference(set(regMetaSidsMap.keys())))
		for sid in notRegMetaSids:
			if currentNilmStatusMap[sid]['hasMeta'] == 1:
				currentNilmStatusMap[sid]['hasMeta'] = 0
				currentNilmStatusMap[sid]['updateTS'] = getUtcNowTS()

		self._logger.info(" - 4) combine currentNilmStatusMap with activated device")
		for sid in activeDeviceSiteMap.keys():
			if not sid in currentNilmStatusMap.keys():
				did = activeDeviceSiteMap[sid]['did']
				deviceCh = activeDeviceSiteMap[sid]['deviceCh']
				deviceFreq = activeDeviceSiteMap[sid]['deviceFreq']
				nilmFreq = activeDeviceSiteMap[sid]['nilmFreq']
				currentNilmStatusMap[sid] = self._getNilmFormat(sid, did, deviceStatus='active', deviceCh=deviceCh, deviceFreq=deviceFreq, nilmFreq=nilmFreq)
			else:
				if currentNilmStatusMap[sid]['deviceStatus'] == 'inactive':
					currentNilmStatusMap[sid]['deviceStatus'] = 'active'
					currentNilmStatusMap[sid]['updateTS'] = getUtcNowTS()

		self._logger.info(" - 5) combine currentNilmStatusMap with inactivated device")
		inactivatedSids = list(set(currentNilmStatusMap.keys()).difference(activeDeviceSiteMap.keys()))
		for sid in inactivatedSids:
			if currentNilmStatusMap.has_key(sid):
				currentNilmStatusMap[sid]['deviceStatus'] = 'inactive'
				currentNilmStatusMap[sid]['updateTS'] = getUtcNowTS()
				
		return currentNilmStatusMap

	def _getNilmFormat(self, sid, did, **kargs):
		updateDate = getUtcNowTS()
		defaultStatusFormat = {'sid': sid, 'did': did, 'updateTS': updateDate}
		defaultArgs = {'deviceCh':1, 'deviceFreq': 15, 'nilmFreq': 15, 'deviceStatus': 'inactive', 'nilmStatus': 'inactive', 'hasMeta': 0, 'regTS': updateDate}

		checkParams = lambda key: kargs[key] if key in kargs.keys() else defaultArgs[key]

		for key in defaultArgs.keys():
			defaultStatusFormat[key] = checkParams(key)
		return defaultStatusFormat

	def _extractToUpdateNilmStatus(self, nilmSiteStatusMap, currentNilmStatusMap):
		changedNilmStatusMap = {}
		nilmSids = nilmSiteStatusMap.keys()

		for sid in currentNilmStatusMap.keys():
			deviceStatus = currentNilmStatusMap[sid]['deviceStatus']
			nilmStatus = currentNilmStatusMap[sid]['nilmStatus']
			hasMeta = currentNilmStatusMap[sid]['hasMeta']

			if sid in nilmSids:
				if not deviceStatus == nilmSiteStatusMap[sid]['deviceStatus'] or not nilmStatus == nilmSiteStatusMap[sid]['nilmStatus'] or not hasMeta == nilmSiteStatusMap[sid]['hasMeta']:
					changedNilmStatusMap[sid] = currentNilmStatusMap[sid]
			else:
				changedNilmStatusMap[sid] = currentNilmStatusMap[sid]
		return changedNilmStatusMap

	def _setDeviceCountryInfo(self, siteCountryCodeInfoMap, nilmStatusMap):
		countryDeviceMetas = getCountryDeviceMetaInfos()

		for sid in nilmStatusMap.keys():
			if siteCountryCodeInfoMap.has_key(sid):
				countryCode = siteCountryCodeInfoMap[sid]
				if countryDeviceMetas.has_key(countryCode):
					nilmStatusMap[sid]['deviceCh'] = countryDeviceMetas[countryCode]['device_channel']
					nilmStatusMap[sid]['deviceFreq'] = countryDeviceMetas[countryCode]['device_frequency']
					nilmStatusMap[sid]['nilmFreq'] = countryDeviceMetas[countryCode]['nilm_frequency']
		return nilmStatusMap
