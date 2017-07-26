#!/usr/bin/env python
# -*- coding: utf-8 -*-

from data_store.DataServiceManager import DataServiceManager
from NilmCoreProcess import NilmCoreProcess
import os

class NilmMetaUpdateLearner(NilmCoreProcess):
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
		metaFilePath = None
		siteInfoFilePath = None
		existedMeta = self._dataManager.getNilmMetaForUpdateInfo(sid, did, lfid)

		if not existedMeta:
			self._logger.warn('# Meta is not be existed. sid : %d' %(sid))
			return self._genResult(dailyBasedTS, sid, did, lfid)

		try:
			metaFilePath = self._fsHandler.saveNilmMetaInfoToFile(feederDataFilePath, existedMeta)
			siteInfoFilePath = self._fsHandler.saveSiteCountryInfoToFile(sid, self._countryCode, self._timezone)
			self.setSidInfo(sid, feederDataFilePath)
			updatedAppMetas = self.runUpdateAppMetas(metaFilePath, siteInfoFilePath, feederDataFilePath)

			if updatedAppMetas:
				convertedNewAppMetas = {}
				for fid in updatedAppMetas.keys():
					metaElement = updatedAppMetas[fid]
					try:
						convertedNewAppMetas[fid] = metaElement
					except TypeError, e:
						self._logger.exception(e)

				nilmMetaInfo = {'sid': sid, 'did': did, 'lfid': lfid, 'metaSearch':{}, 'metaUpdate':{}}

				# new meta
				insertNewMetaNfids = list(set(convertedNewAppMetas.keys()).difference(existedMeta.keys()))
				for nfid in insertNewMetaNfids:
					vfidMeta = convertedNewAppMetas[str(nfid)]
					appType = self._getApplianceType(vfidMeta)
					nilmMetaInfo['metaSearch'][int(nfid)] = self._genFidMetaSet(appType, vfidMeta)
					del convertedNewAppMetas[nfid]

				for nfid in convertedNewAppMetas.keys():
					targetMetaInfo = convertedNewAppMetas[str(nfid)]
					existedMetaInfo = existedMeta[str(nfid)]
					if targetMetaInfo != existedMetaInfo:
						vfidMeta = existedMeta[str(nfid)]
						if targetMetaInfo['status'] == 'enabled':
							status = 1
						else:
							status = 0
						nilmMetaInfo['metaUpdate'][int(nfid)] = self._genUpdateMetaSet(vfidMeta, status)
				addedNewMetaList = insertNewMetaNfids
				updatedNewMetaList = convertedNewAppMetas.keys()
				addedNewMetaList.sort()
				updatedNewMetaList.sort()
				self._logger.debug("# Add new meta fids : %s" %(str(addedNewMetaList)))
				self._logger.debug("# Update meta fids  : %s" %(str(updatedNewMetaList)))
				self._dataManager.setNilmMetaUpdateInfo(nilmMetaInfo)
		except Exception, e:
			self._logger.exception(e)

		self._logger.info('# remove meta and site info files')
		if metaFilePath:
			os.remove(metaFilePath)
		if siteInfoFilePath:
			os.remove(siteInfoFilePath)

		self._logger.info('# completed meta update')
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

	def _genFidMetaSet(self, appType, vfidMeta, status=1):
		metaInfoSet = {
			'appType' : appType,
			'vfidMeta' : vfidMeta,
			'status': status
		}
		return metaInfoSet

	def _genUpdateMetaSet(self, metaInfo, status):
		metaInfoSet = {
			'vfidMeta' : metaInfo,
			'status' : status
		}
		return metaInfoSet

	def _getApplianceType(self, vfidMeta):
		if vfidMeta.has_key('encored_appliance_type'):
			appType = vfidMeta['encored_appliance_type']
		else:
			appType = 71
		return appType
