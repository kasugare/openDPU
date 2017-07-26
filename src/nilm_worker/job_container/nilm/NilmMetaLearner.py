#!/usr/bin/env python
# -*- coding: utf-8 -*-

from data_store.DataServiceManager import DataServiceManager
from NilmCoreProcess import NilmCoreProcess
import os

class NilmMetaLearner(NilmCoreProcess):
	def __init__(self, logger, rLogger, jobId, jobType, countryCode, timezone, fsHandler, debugMode):
		NilmCoreProcess.__init__(self, logger, rLogger, debugMode)
		self._logger = logger
		self._jobId = jobId
		self._jobType = jobType
		self._countryCode = countryCode
		self._timezone = timezone
		self._fsHandler = fsHandler
		self._dataManager = DataServiceManager(logger)

	def doProcess(self, sid, did, lfid, feederDataFilePath, dailyBasedTS = None):
		self._logger.info("# Do nilm meta search, sid : %d, did : %d, lfid : %d, data : %s, country code : %s, timezone : %s" %(sid, did, lfid, feederDataFilePath, self._countryCode, self._timezone))
		
		siteInfoFilePath = None

		try:
			self.setSidInfo(sid, feederDataFilePath)
			siteInfoFilePath = self._fsHandler.saveSiteCountryInfoToFile(sid, self._countryCode, self._timezone)

			newAppMetas = self.runLearnAppMetas(siteInfoFilePath, feederDataFilePath)
			if newAppMetas:
				nilmMetaInfo = {'sid': sid, 'did': did, 'lfid': lfid, 'metaSearch':{}}
				nilmMetaInfo['metaSearch'][999] = self._genFidMetaSet(71)
				newAppMetaList = [newAppMetas[metaKey] for metaKey in newAppMetas.keys()]

				for newAppMeta in newAppMetaList:
					nfid = newAppMeta['fid']
					appType = self._getApplianceType(newAppMeta)
					nilmMetaInfo['metaSearch'][nfid] = self._genFidMetaSet(appType, newAppMeta)
				self._dataManager.setNilmMetaSearchInfo(nilmMetaInfo)
		except Exception, e:
			self._logger.exception(e)

		self._logger.info('# remove site info file')
		if siteInfoFilePath:
			os.remove(siteInfoFilePath)

		self._logger.info('# completed meta search')
		return self._genResult(dailyBasedTS, sid, did, lfid)

	def _genResult(self, dailyBasedTS, sid, did, lfid):
		resultMap = {
			'dbTS': dailyBasedTS,
			'jobType': self._jobType,
			'sid': sid,
			'did': did,
			'lfid': lfid
		}
		return resultMap

	def _genFidMetaSet(self, appType, vfidMeta = None):
		metaInfoSet = {
			'appType' : appType,
			'vfidMeta' : vfidMeta
		}
		return metaInfoSet

	def _getApplianceType(self, vfidMeta):
		if vfidMeta.has_key('encored_appliance_type'):
			appType = vfidMeta['encored_appliance_type']
		else:
			appType = 71
		return appType

