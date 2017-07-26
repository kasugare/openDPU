#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_system import getNilmMetaUpdatePeriod
from nilm_worker.job_container.nilm_status_checker.database.tajo.TajoHandler import TajoHandler
from nilm_worker.job_container.nilm_status_checker.database.rds.RdsHandler import RdsHandler


class DataServiceManager:
	def __init__(self, logger):
		self._logger = logger
		self._dbHandler = RdsHandler(logger)
		self._tajoHandler = TajoHandler(logger)

	def getTargetNilmSiteMap(self, gids):
		groupSiteMap = self._dbHandler.selectGroupSiteMapQuery(gids)
		return groupSiteMap

	def getRegistratedNilmMetaSids(self):
		regMetaSidsMap = self._dbHandler.selectRegistratedMetaSidsQuery()
		return regMetaSidsMap

	def getActiveDeviceSiteMap(self, startTS, endTS):
		deviceSiteMap = self._tajoHandler.selectActiveDeviceSiteMap(startTS, endTS)
		return deviceSiteMap

	def getSiteStatusMap(self):
		sidsStatusMap = self._dbHandler.selectSiteStatusQuery()
		return sidsStatusMap

	def getSiteCountryCodeInfos(self, sids):
		siteCountryCodeInfoMap = self._dbHandler.selectSiteCountryCodeQuery(sids)
		return siteCountryCodeInfoMap

	def setNilmStatusQuery(self, changedNilmStatusMap):
		updateSet = []
		for sid in changedNilmStatusMap.keys():
			changedNilmStatus = changedNilmStatusMap[sid]
			did = int(changedNilmStatus['did'])
			deviceCh = int(changedNilmStatus['deviceCh'])
			deviceFreq = int(changedNilmStatus['deviceFreq'])
			nilmFreq = int(changedNilmStatus['nilmFreq'])
			deviceStatus = changedNilmStatus['deviceStatus']
			nilmStatus = changedNilmStatus['nilmStatus']
			hasMeta = int(changedNilmStatus['hasMeta'])
			regTS = int(changedNilmStatus['regTS'])
			updateTS = int(changedNilmStatus['updateTS'])
			updateSet.append((int(sid), did, deviceCh, deviceFreq, nilmFreq, deviceStatus, nilmStatus, hasMeta, regTS, updateTS))
		try:
			self._dbHandler.insertNilmStatusQuery(updateSet)
		except Exception, e:
			for updateInfo in updateSet:
				self._logger.error(updateInfo)
			self._logger.exception(e)


