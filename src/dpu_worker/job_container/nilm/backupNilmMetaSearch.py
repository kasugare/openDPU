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
        self._logger.info("# [%s] Do nilm meta update, sid : %d, did : %d, lfid : %d, data : %s, freq : %d hz" %(self._jobId, sid, did, lfid, feederDataFilePath, hz))

        oldMetaInfo = self._dataManager.getNilmMetaInfo(sid, did, lfid)

        hasMiscValue = False
        metaDetector = AppDetectR(self._logger)

        try:
            metaDetector.set_data(feederDataFilePath, hz)
            isDetected = metaDetector.detect_apps()
            self._logger.info("- [%s] meta detected : %s, freq : %d hz" %(self._jobId, str(isDetected), hz))

            newAppMeta = json.loads(metaDetector.getAppInfo())
            # self._dataManager.setUsageRouteDataService(sid)

            if not newAppMeta:
                self._dataManager.setNilmVirtualFeederInfo(sid, did, lfid, 999)
                return

            if isDetected:
                existingMetaInfo = self._dataManager.getNilmMetaInfo(sid, did, lfid)

                if not existingMetaInfo:
                    for nfid in [int(nfid) for nfid in newAppMeta.keys()]:
                        vfidMeta = newAppMeta[str(nfid)]
                        applianceType = self._getApplianceType(vfidMeta)

                        vfid = self._dataManager.setNilmVirtualFeederInfo(sid, did, lfid, nfid, applianceType)
                        self._dataManager.setNilmMetaInfo(vfid, vfidMeta)
                        if not hasMiscValue:
                            self._dataManager.setNilmVirtualFeederInfo(sid, did, lfid, 999)
                            hasMiscValue = True
            else:
                if newAppMeta:
                    for nfid in [int(nfid) for nfid in newAppMeta.keys()]:
                        vfidMeta = newAppMeta[str(nfid)]
                        applianceType = self._getApplianceType(vfidMeta)
                        vfid = self._dataManager.setNilmVirtualFeederInfo(sid, did, lfid, nfid, applianceType)
                        if vfid:
                            self._dataManager.setNilmMetaInfo(vfid, newAppMeta[str(nfid)])
                if not hasMiscValue:
                    self._dataManager.setNilmVirtualFeederInfo(sid, did, lfid, 999)
                    hasMiscValue = True
        except Exception, e:
            self._logger.exception(e)
        finally:
            metaDetector.pid.terminate()

        resultMap = {
            'dbTS': dailyBasedTS,
            'jobType': self._jobType,
            'sid': sid,
            'did': did,
            'lfid': lfid,
            'hz': hz
        }
        return resultMap

    def _getApplianceType(self, vfidMeta):
        if vfidMeta.has_key('encored_appliance_type'):
            applianceType = vfidMeta['encored_appliance_type']
        else:
            applianceType = 71
        return applianceType

