#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_system import getDpuMetaSearchPeriod, getDpuMetaUpdatePeriod
from common.util_datetime import tzHtime
from protocol.message_pool.MessageGenerator import genReqGenDpuRawData, genReqDpuStatusCheck, genReqDpuLearnerSchedule

class CollectionMessageGenerator:
	def __init__(self, logger):
		self._logger = logger

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
			fromTS = startTS - getDpuMetaSearchPeriod()
			fromUpdateTS = startTS - getDpuMetaUpdatePeriod()
			toTS = endTS - (24 * 60 * 60 * 1000)
			if 'meta' in jobs:
				metaSearchInfo = self._getTargetMetaSearchInfo(gids, sids, fromSearchTS = fromTS, fromUpdateTS = fromUpdateTS, forced = forced)
			if 'update' in jobs:
				metaUpdateInfo = self._getTargetMetaUpdateSearchInfo(gids, sids, fromSearchTS = fromTS, fromUpdateTS = fromUpdateTS, forced = forced)


		dpuCountryInfoMap = self._dataManager.getDpuCountryInfo(sids)
		dpuStatusCheckParams = []
		dpuTargetInfoParams = []

		if 'learnschedule' in jobs:
			genReqDpuLearnerSchedule
			jobParams = self._cvtLearningScheduleMsgParams('learnSchedule')
			protoMsgs.append(genReqDpuLearnerSchedule('DPU_LEARN_SCHEDULE', jobParams))

		if 'status' in jobs:
			dpuSiteStatusCheckerParams = self._cvtStatusMsgParams('statusCheck', startTS, endTS, gids)
			dpuStatusCheckParams.extend(dpuSiteStatusCheckerParams)

		if 'meta' in jobs and metaSearchInfo:
			metaSearchSiteParams = self._cvtMsgParams(fromTS, toTS, 'metaSearch', metaSearchInfo, dpuCountryInfoMap, debugMode=debugMode)
			dpuTargetInfoParams.extend(metaSearchSiteParams)

		if 'update' in jobs and metaUpdateInfo:
			metaUpdateSiteParams = self._cvtMsgParams(fromTS, toTS, 'metaUpdate', metaUpdateInfo, dpuCountryInfoMap, debugMode=debugMode)
			dpuTargetInfoParams.extend(metaUpdateSiteParams)

		if 'usage' in jobs:
			activeSiteInfo = self._getUsageSiteInfo(sids, forced=forced)
			if activeSiteInfo:
				activeSiteParams = self._cvtMsgParams(startTS, endTS, 'usage', activeSiteInfo, dpuCountryInfoMap, rExtOpt=rExtOpt, debugMode=debugMode)
				dpuTargetInfoParams.extend(activeSiteParams)

		if 'candidate' in jobs:
			candidateSiteInfo = self._getCandidateSiteInfo(sids)
			if candidateSiteInfo:
				candidateSiteParams = self._cvtMsgParams(startTS, endTS, 'candidate', candidateSiteInfo, dpuCountryInfoMap, debugMode=debugMode)
				dpuTargetInfoParams.extend(candidateSiteParams)

		for statusCheckParam in dpuStatusCheckParams:
			protoMsgs.append(genReqDpuStatusCheck('TAJO', 'DPU_STATUS_CHECK', statusCheckParam))

		for sitesParam in dpuTargetInfoParams:
			protoMsgs.append(genReqGenDpuRawData('TAJO', 'edm3', 'DPU_RAW_DATA', 'HDFS', sitesParam))

		self._printMsgParams(protoMsgs)
		return protoMsgs

	def _getUsageSiteInfo(self, sids, forced=False):
		activeSiteInfo = self._dataManager.getActiveDpuInfo(sids, forced=forced)
		return activeSiteInfo

	def _getCandidateSiteInfo(self, sids):
		candidateSiteInfo = self._dataManager.getCandidateDpuInfo(sids)
		return candidateSiteInfo

	def _getTargetMetaSearchInfo(self, gids, sids, **args):
		metaSearchInfo = None
		forced = args['forced']

		if args.has_key('fromSearchTS'):
			fromSearchTS = args['fromSearchTS']
			hasMeta = 0
			metaSearchInfo = self._dataManager.getDpuMetaSearchInfo(gids, sids, fromSearchTS, forced, hasMeta)
		return metaSearchInfo

	def _getTargetMetaUpdateSearchInfo(self, gids, sids, **args):
		metaUpdateInfo = None
		forced = args['forced']

		if args.has_key('fromUpdateTS'):
			fromUpdateTS = args['fromUpdateTS']
			hasMeta = 1
			metaUpdateInfo = self._dataManager.getDpuMetaUpdateSearchInfo(gids, sids, fromUpdateTS, forced, hasMeta)
		return metaUpdateInfo

	def _cvtMsgParams(self, startTS, endTS, analysisType, siteInfo, dpuCountryInfoMap, rExtOpt=None, debugMode=False):
		def _targetSiteMap(targetSids, siteInfo):
			targetSidMap = {}
			for sid in targetSids:
				if siteInfo.has_key(sid):
					targetSidMap[sid] = siteInfo[sid]
			return targetSidMap

		dpuParams = []
		for countryCode in dpuCountryInfoMap.keys():
			sidsInCountry = dpuCountryInfoMap[countryCode]['sids']
			dpuSids = siteInfo.keys()
			targetSids = list(set(dpuSids).intersection(set(sidsInCountry)))

			if targetSids:
				params = {
					'analysisType': analysisType,
					'startTS': startTS,
					'endTS': endTS,
					'country': countryCode,
					'timezone': dpuCountryInfoMap[countryCode]['TZ'],
					'deviceCh': dpuCountryInfoMap[countryCode]['deviceCh'],
					'deviceFreq': dpuCountryInfoMap[countryCode]['deviceFreq'],
					'dpuFreq': dpuCountryInfoMap[countryCode]['dpuFreq'],
					'target': _targetSiteMap(targetSids, siteInfo),
					'rExtOpt': rExtOpt,
					'debugMode': debugMode
				}
				dpuParams.append(params)
		return dpuParams

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
				if param.has_key('dpuFreq'): self._logger.info('  - dpu freq      : %d' %(param['dpuFreq']))
				if param.has_key('startTS'): self._logger.info('  - startTS     : [%s] : %d' %(tzHtime(param['startTS']), param['startTS']))
				if param.has_key('endTS'): self._logger.info('  - endTS       : [%s] : %d' %(tzHtime(param['endTS']), param['endTS']))
				if param.has_key('target'): self._logger.info('  - size of tar : %d' %(len(param['target'])))
				if len(protoMsgs)-1 != index:
					self._logger.info('-' * 71)
		self._logger.info('=' * 71)
