#!/usr/bin/env python
# -*- coding: utf-8 -*-

from nilm_core.app_info_comp import AppDistanceComp
from nilm_core.app_detect import AppDetectR
from data_store.DataServiceManager import DataServiceManager
import traceback
import pandas
import json

NILM_COLUMN_SET = ['timestamp','voltage','current','active_power','reactive_power','apparent_power','power_factor']

class NilmMetaSearch:
	def __init__(self, logger, jobId, jobType):
		self._logger = logger
		self._jobId = jobId
		self._jobType = jobType
		self._dataManager = DataServiceManager(logger)

	def doProcess(self, hz, sid, did, lfid, feederDataFilePath, dailyBasedTS = None):
		self._logger.info("# [%s] Do nilm meta search, sid : %d, did : %d, lfid : %d, data : %s, freq : %d hz" %(self._jobId, sid, did, lfid, feederDataFilePath, hz))

		metaDetector = AppDetectR(self._logger)
		try:
			metaDetector.set_data(feederDataFilePath, hz)
			isDetected = metaDetector.detect_apps()
			newAppMeta = json.loads(metaDetector.getAppInfo())

			if isDetected:
				nilmMetaInfo = {'sid': sid, 'did': did, 'lfid': lfid, 'metaSearch':{}}
				nilmMetaInfo['metaSearch'][999] = self._genFidMetaSet(71, None)
				if newAppMeta:
					for nfid in [int(nfid) for nfid in newAppMeta.keys()]:
						vfidMeta = newAppMeta[str(nfid)]
						appType = self._getApplianceType(vfidMeta)
						nilmMetaInfo['metaSearch'][nfid] = self._genFidMetaSet(appType, vfidMeta)
				self._dataManager.setNilmMetaSearchInfo(nilmMetaInfo)
		except Exception, e:
			self._logger.exception(e)
		finally:
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

	def _getApplianceType(self, vfidMeta):
		if vfidMeta.has_key('encored_appliance_type'):
			appType = vfidMeta['encored_appliance_type']
		else:
			appType = 71
		return appType

