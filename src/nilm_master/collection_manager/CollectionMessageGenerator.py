#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_system import getNilmMetaSearchPeriod, getNilmMetaUpdatePeriod
from common.util_datetime import tzHtime
from protocol.message_pool.MessageGenerator import genReqGenNilmRawData, genReqNilmStatusCheck, genReqNilmLearnerSchedule
from data_store.DataServiceManager import DataServiceManager

class CollectionMessageGenerator:
	def __init__(self, logger):
		self._logger = logger
		self._dataManager = DataServiceManager(logger)

	def genCollectionMessages(self, userParams):
		jobs = userParams['jobs']
		gids = userParams['gids']
		sids = userParams['sids']
		startTS = userParams['startTS']
		endTS = userParams['endTS']
		forced = userParams['forced']
		rExtOpt = userParams['rExtOpt']
		debugMode = userParams['debugMode']
		protoMsgs = []

		self._printUserParams(userParams)

		if forced:
			fromTS = startTS
			toTS = endTS
			if 'meta' in jobs:
				metaSearchInfo = self._getTargetMetaSearchInfo(gids, sids, fromSearchTS = startTS, fromUpdateTS = startTS, forced = forced)
			if 'update' in jobs:
				metaUpdateInfo = self._getTargetMetaUpdateSearchInfo(gids, sids, fromSearchTS = startTS, fromUpdateTS = startTS, forced = forced)
		else:
			fromTS = startTS - getNilmMetaSearchPeriod()
			fromUpdateTS = startTS - getNilmMetaUpdatePeriod()
			toTS = endTS - (24 * 60 * 60 * 1000)
			if 'meta' in jobs:
				metaSearchInfo = self._getTargetMetaSearchInfo(gids, sids, fromSearchTS = fromTS, fromUpdateTS = fromUpdateTS, forced = forced)
			if 'update' in jobs:
				metaUpdateInfo = self._getTargetMetaUpdateSearchInfo(gids, sids, fromSearchTS = fromTS, fromUpdateTS = fromUpdateTS, forced = forced)
				

		nilmCountryInfoMap = self._dataManager.getNilmCountryInfo(sids)
		nilmStatusCheckParams = []
		nilmTargetInfoParams = []

		if 'learnschedule' in jobs:
			genReqNilmLearnerSchedule
			jobParams = self._cvtLearningScheduleMsgParams('learnSchedule')
			protoMsgs.append(genReqNilmLearnerSchedule('NILM_LEARN_SCHEDULE', jobParams))

		if 'status' in jobs:
			nilmSiteStatusCheckerParams = self._cvtStatusMsgParams('statusCheck', startTS, endTS, gids)
			nilmStatusCheckParams.extend(nilmSiteStatusCheckerParams)

		if 'meta' in jobs and metaSearchInfo:
			metaSearchSiteParams = self._cvtMsgParams(fromTS, toTS, 'metaSearch', metaSearchInfo, nilmCountryInfoMap, debugMode=debugMode)
			nilmTargetInfoParams.extend(metaSearchSiteParams)

		if 'update' in jobs and metaUpdateInfo:
			metaUpdateSiteParams = self._cvtMsgParams(fromTS, toTS, 'metaUpdate', metaUpdateInfo, nilmCountryInfoMap, debugMode=debugMode)
			nilmTargetInfoParams.extend(metaUpdateSiteParams)

		if 'usage' in jobs:
			activeSiteInfo = self._getUsageSiteInfo(sids, forced=forced)
			if activeSiteInfo:
				activeSiteParams = self._cvtMsgParams(startTS, endTS, 'usage', activeSiteInfo, nilmCountryInfoMap, rExtOpt=rExtOpt, debugMode=debugMode)
				nilmTargetInfoParams.extend(activeSiteParams)

		if 'candidate' in jobs:
			candidateSiteInfo = self._getCandidateSiteInfo(sids)
			if candidateSiteInfo:
				candidateSiteParams = self._cvtMsgParams(startTS, endTS, 'candidate', candidateSiteInfo, nilmCountryInfoMap, debugMode=debugMode)
				nilmTargetInfoParams.extend(candidateSiteParams)

		for statusCheckParam in nilmStatusCheckParams:
			protoMsgs.append(genReqNilmStatusCheck('TAJO', 'NILM_STATUS_CHECK', statusCheckParam))

		for sitesParam in nilmTargetInfoParams:
			protoMsgs.append(genReqGenNilmRawData('TAJO', 'edm3', 'NILM_RAW_DATA', 'HDFS', sitesParam))

		self._printMsgParams(protoMsgs)
		return protoMsgs

	def _getUsageSiteInfo(self, sids, forced=False):
		activeSiteInfo = self._dataManager.getActiveNilmInfo(sids, forced=forced)
		return activeSiteInfo

	def _getCandidateSiteInfo(self, sids):
		candidateSiteInfo = self._dataManager.getCandidateNilmInfo(sids)
		return candidateSiteInfo

	def _getTargetMetaSearchInfo(self, gids, sids, **args):
		metaSearchInfo = None
		forced = args['forced']

		if args.has_key('fromSearchTS'):
			fromSearchTS = args['fromSearchTS']
			hasMeta = 0
			metaSearchInfo = self._dataManager.getNilmMetaSearchInfo(gids, sids, fromSearchTS, forced, hasMeta)
		return metaSearchInfo

	def _getTargetMetaUpdateSearchInfo(self, gids, sids, **args):
		metaUpdateInfo = None
		forced = args['forced']

		if args.has_key('fromUpdateTS'):
			fromUpdateTS = args['fromUpdateTS']
			hasMeta = 1
			metaUpdateInfo = self._dataManager.getNilmMetaUpdateSearchInfo(gids, sids, fromUpdateTS, forced, hasMeta)
		return metaUpdateInfo

	def _cvtMsgParams(self, startTS, endTS, analysisType, siteInfo, nilmCountryInfoMap, rExtOpt=None, debugMode=False):
		def _targetSiteMap(targetSids, siteInfo):
			targetSidMap = {}
			for sid in targetSids:
				if siteInfo.has_key(sid):
					targetSidMap[sid] = siteInfo[sid]
			return targetSidMap

		nilmParams = []
		for countryCode in nilmCountryInfoMap.keys():
			sidsInCountry = nilmCountryInfoMap[countryCode]['sids']
			nilmSids = siteInfo.keys()
			targetSids = list(set(nilmSids).intersection(set(sidsInCountry)))

			if targetSids:
				params = {
					'analysisType': analysisType,
					'startTS': startTS,
					'endTS': endTS,
					'country': countryCode,
					'timezone': nilmCountryInfoMap[countryCode]['TZ'],
					'deviceCh': nilmCountryInfoMap[countryCode]['deviceCh'],
					'deviceFreq': nilmCountryInfoMap[countryCode]['deviceFreq'],
					'nilmFreq': nilmCountryInfoMap[countryCode]['nilmFreq'],
					'target': _targetSiteMap(targetSids, siteInfo),
					'rExtOpt': rExtOpt,
					'debugMode': debugMode
				}
				nilmParams.append(params)
		return nilmParams

	def _cvtStatusMsgParams(self, analysisType, startTS, endTS, gids):
		params = {
			'analysisType': analysisType,
			'startTS': startTS,
			'endTS': endTS,
			'gids': gids
		}
		return [params]

	def _cvtLearningScheduleMsgParams(self, analysisType):
		params = {
			'analysisType': analysisType
		}
		return params

	def _printUserParams(self, userParams):
		jobs = userParams['jobs']
		gids = userParams['gids']
		sids = userParams['sids']
		startTS = userParams['startTS']
		endTS = userParams['endTS']
		self._logger.info('='*60)
		self._logger.info(' - jobs    : %s' %(str(jobs)))
		self._logger.info(' - gids    : %s' %(str(gids)))
		if len(sids) > 10:
			strSids = (', ').join([str(sid) for sid in sids[0:10]])
			self._logger.info(' - sids    : [%s, ... ](%d)' %(strSids, len(sids)))
		else:
			self._logger.info(' - sids    : %s' %(str(sids)))
		self._logger.info(' - startTS : [%s] : %d' %(tzHtime(startTS), startTS))
		self._logger.info(' - endTS   : [%s] : %d' %(tzHtime(endTS), endTS))
		self._logger.info('='*60)

	def _printMsgParams(self, protoMsgs):
		self._logger.info('=' * 71)
		for index in range(len(protoMsgs)):
			msgParams = protoMsgs[index]
			jobType = msgParams['jobType']
			if msgParams.has_key('params'):
				param = msgParams['params']
				if param.has_key('country'): self._logger.info('< job type : %s , contray code : %s, timezone : %s >' %(jobType, param['country'], param['timezone']))
				else:
					self._logger.info('< job type : %s >' %(jobType))
				if param.has_key('analysisType'): self._logger.info(' # analysis type   : %s' %param['analysisType'])
				if param.has_key('deviceCh'): self._logger.info('  - device channel : %d' %(param['deviceCh']))
				if param.has_key('deviceFreq'): self._logger.info('  - device freq    : %d' %(param['deviceFreq']))
				if param.has_key('nilmFreq'): self._logger.info('  - nilm freq      : %d' %(param['nilmFreq']))
				if param.has_key('startTS'): self._logger.info('  - startTS     : [%s] : %d' %(tzHtime(param['startTS']), param['startTS']))
				if param.has_key('endTS'): self._logger.info('  - endTS       : [%s] : %d' %(tzHtime(param['endTS']), param['endTS']))
				if param.has_key('target'): self._logger.info('  - size of tar : %d' %(len(param['target'])))
				if len(protoMsgs)-1 != index:
					self._logger.info('-' * 71)
		self._logger.info('=' * 71)
