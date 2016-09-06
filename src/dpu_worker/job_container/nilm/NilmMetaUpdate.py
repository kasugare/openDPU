#!/usr/bin/env python
# -*- coding: utf-8 -*-

from nilm_core.app_info_comp import AppDistanceComp
from nilm_core.app_detect import AppDetectR
from data_store.DataServiceManager import DataServiceManager
import traceback
import pandas
import json

NILM_COLUMN_SET = ['timestamp','voltage','current','active_power','reactive_power','apparent_power','power_factor']

class NilmMetaUpdate:
	def __init__(self, logger, jobId, jobType):
		self._logger = logger
		self._jobId = jobId
		self._jobType = jobType
		self._dataManager = DataServiceManager(logger)

	def doProcess(self, hz, sid, did, lfid, feederDataFilePath, dailyBasedTS = None):
		self._logger.info("# [%s] Do nilm meta search, sid : %d, did : %d, lfid : %d, data : %s, freq : %d hz" %(self._jobId, sid, did, lfid, feederDataFilePath, hz))

		existedMeta = self._dataManager.getNilmMetaInfo(sid, did, lfid)
		metaDetector = AppDetectR(self._logger)
		if not existedMeta:
			self._logger.warn('# Meta is not be existed. sid : %d' %(sid))
			return self._genResult(dailyBasedTS, sid, did, lfid, hz)
		try:
			metaDetector.setExistedMeta(existedMeta)
			metaDetector.set_data(feederDataFilePath, hz)
			isDetected = metaDetector.detect_apps()
			if isDetected:
				newAppMeta = json.loads(metaDetector.getAppInfo())
				nilmMetaInfo = {'sid': sid, 'did': did, 'lfid': lfid, 'metaSearch':{}, 'metaUpdate':{}}

				# new meta
				insertNewMetaNfds  = list(set(newAppMeta.keys()).difference(existedMeta.keys()))
				for nfid in insertNewMetaNfds:
					vfidMeta = newAppMeta[str(nfid)]
					appType = self._getApplianceType(vfidMeta)
					nilmMetaInfo['metaSearch'][int(nfid)] = self._genFidMetaSet(appType, vfidMeta)

				# update meta info
				updateNewMetaNfids = list(set(newAppMeta.keys()).intersection(existedMeta.keys()))
				for nfid in updateNewMetaNfids:
					vfidMeta = newAppMeta[str(nfid)]
					isEnabled = 1
					nilmMetaInfo['metaUpdate'][int(nfid)] = self._genUpdateMetaSet(vfidMeta, isEnabled)

				# not use meta info
				updateDisableNfids = list(set(existedMeta.keys()).difference(newAppMeta.keys()))
				for nfid in updateDisableNfids:
					vfidMeta = existedMeta[str(nfid)]
					isEnabled = 0
					nilmMetaInfo['metaUpdate'][int(nfid)] = self._genUpdateMetaSet(vfidMeta, isEnabled)
				self._dataManager.setNilmMetaUpdateInfo(nilmMetaInfo)
		except Exception, e:
			self._logger.exception(e)
		finally:
			if metaDetector.pid:
				metaDetector.pid.terminate()

		self._logger.info('# completed meta search')
		return self._genResult(dailyBasedTS, sid, did, lfid, hz)

	def _genResult(self, dailyBasedTS, sid, did, lfid, hz):
		resultMap = {
			'dbTS': dailyBasedTS,
			'jobType': self._jobType,
			'sid': sid,
			'did': did,
			'lfid': lfid,
			'hz': hz
		}
		return resultMap

	def _genFidMetaSet(self, appType, vfidMeta = None):
		metaInfoSet = {
			'appType' : appType,
			'vfidMeta' : vfidMeta
		}
		return metaInfoSet

	def _genUpdateMetaSet(self, metaInfo, isEnabled=1):
		metaInfoSet = {
			'vfidMeta' : metaInfo,
			'isEnabled' : isEnabled
		}
		return metaInfoSet

	def _getApplianceType(self, vfidMeta):
		if vfidMeta.has_key('encored_appliance_type'):
			appType = vfidMeta['encored_appliance_type']
		else:
			appType = 71
		return appType
