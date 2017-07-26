#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_datastore import getDBInfo
from common.conf_system import getSystemEnv
from common.conf_country import getCountryDeviceMetaInfos
from common.util_datetime import getUtcNowTS
from common.util_common import convertDictToString
from RdsQueryPool import RdsQueryPool
import MySQLdb.cursors
import MySQLdb
import traceback
import random
import time
import json
import ast

class RdsHandler:
	def __init__(self, logger):
		self.queryPool = RdsQueryPool(logger)
		self._logger = logger
		self._maxRetry = 5

	def _connectDb(self):
		rdsConfig = getDBInfo()
		dbConn = MySQLdb.connect(
			host = rdsConfig['host'],
			port = int(rdsConfig['port']),
			user = rdsConfig['user'],
			passwd = rdsConfig['password'],
			db = rdsConfig['database'],
			cursorclass = MySQLdb.cursors.DictCursor
		)
		return dbConn

	def _executeWriteQuery(self, queryString):
		lastId = None
		retryCnt = 0
		while retryCnt < self._maxRetry:
			try:
				dbConn = self._connectDb()
				dbCursor = dbConn.cursor()
				dbCursor.execute(queryString)
				dbConn.commit()
				lastId = dbCursor.lastrowid
				dbConn.close()
				break
			except Exception, e:
				if retryCnt < self._maxRetry:
					self._logger.error("# It going to retry to read data : %s" %queryString)
				self._logger.exception(e)
				dbConn.rollback()
				retryCnt += 1
				time.sleep(float(random.randrange(10,30))/10)
		return lastId

	def _executeReadQuery(self, queryString):
		retryCnt = 0
		while retryCnt < self._maxRetry:
			try:
				dbConn = self._connectDb()
				dbCursor = dbConn.cursor()
				dbCursor.execute(queryString)
				if dbCursor.rowcount <= 0:
					return None
				items = dbCursor.fetchall()
				dbConn.close()
				break
			except Exception, e:
				if retryCnt < self._maxRetry:
					self._logger.error("# It going to retry to read data : %s" %queryString)
				self._logger.exception(e)
				retryCnt += 1
				time.sleep(float(random.randrange(10,30))/10)
		return items

	def _executeRemoveQuery(self, queryStrings):
		try:
			dbConn = self._connectDb()
			dbCursor = dbConn.cursor()
			for queryString in queryStrings:
				dbCursor.execute(queryString)
			dbCursor.commit()
			dbConn.close()
		except Exception, e:
			dbConn.rollback()
			self._logger.exception(e)

	# device table
	def selectEdmTypeQuery(self, sid):
		queryString = self.queryPool.getSelectEdmTypeQuery(sid)
		items = self._executeReadQuery(queryString)
		if items:
			if len(items) == 1:
				return items[0]['inst']
		return None

	# nilm_virtual_feeder table
	def checkDupNilmVirtualFeederInfo(self, sid, did, lfid, nfid):
		queryString = self.queryPool.getCheckNilmVirtualFeederQuery(sid, did, lfid, nfid)
		items = self._executeReadQuery(queryString)
		if items:
			return True, int(items[0]['vfid'])
		return False, 'None'

	def selectNilmVirtualFeederInfo(self, sid, did, lfid, nfid):
		queryString = self.queryPool.getSelectNilmVirtualFeederQuery(sid, did, lfid)
		items = self._executeReadQuery(queryString)

		metaInfoMap = {}
		if items:
			for item in items:
				metaInfoMap[item['nfid']] = item['metaInfo']
		return metaInfoMap

	def selectNilmVirtualFeederId(self, sid, did, lfid, nfid):
		queryString = self.queryPool.getSelectNilmVirtualFeederIdQuery(sid, did, lfid, nfid)
		items = self._executeReadQuery(queryString)
		if items and len(items) == 1:
			vfid = items[0]['vfid']
			return vfid
		return None

	def selectNilmVirtualFeederMap(self, sid, did, lfid):
		queryString = self.queryPool.getSelectNilmVirtualFeederMap(sid, did, lfid)
		items = self._executeReadQuery(queryString)

		vfidMap = {}
		if items:
			for item in items:
				vfidMap[item['nfid']] = item['vfid']
		return vfidMap

	def selectNilmSitesVirtualFeederMap(self, sids):
		queryString = self.queryPool.getSelectNilmSitesVirtualFeederMap(sids)
		items = self._executeReadQuery(queryString)

		siteVfidMap = {}
		if items:
			for item in items:
				sid = int(item['sid'])
				nfid = item['nfid']
				vfid = item['vfid']
				if siteVfidMap.has_key(sid):
					siteVfidMap[sid][nfid] = vfid
				else:
					siteVfidMap[sid] = {nfid:vfid}
		return siteVfidMap

	def selectApplianceIdsInfo(self, sid):
		queryString = self.queryPool.getSelectApplianceIdsQuery(sid, did, lfid)
		items = self._executeReadQuery(queryString)

		applianceIds = []
		if items:
			for item in items:
				applianceIds.append(item['id'])
		return applianceIds

	def insertNilmVirtualFeederInfo(self, sid, did, lfid, nfid, appType):
		isDup, vfid = self.checkDupNilmVirtualFeederInfo(sid, did, lfid, nfid)
		if not isDup:
			name = '%d-%d' %(lfid, nfid)
			applianceId = self.insertAppliance(sid, appType, name)
			queryString = self.queryPool.getInsertNilmVirtualFeederInfoQuery(sid, did, lfid, nfid, applianceId)
			vfid = self._executeWriteQuery(queryString)
		return vfid

	def updateNilmVirtualFeederForApplianceDelete(self, sid):
		queryString = self.queryPool.getUpdateNilmVirtualFeederForApplianceDeleteQuery(sid)
		self._executeWriteQuery(queryString)

	def deleteNilmVirtualFeederInfo(self, sid):
		queryString = self.queryPool.getDeleteNilmVirtualFeederInfoQuery(sid)
		self._executeWriteQuery(queryString)


	# nilm_meta table
	def checkDupNilmMetaInfo(self, vfid):
		queryString = self.queryPool.getCheckNilmMetaInfQuery(sid, did, lfid, nfid)
		item = self._executeReadQuery(queryString)[0]
		if int(item['count']) == 0:
			return False, 'None'
		else:
			return True, int(item['vfid'])

	def selectNilmMetaInfo(self, sid, did, lfid):
		queryString = self.queryPool.getSelectNilmMetaInfoQuery(sid, did, lfid)
		items = self._executeReadQuery(queryString)

		if items:
			metaInfoMap = {}
			for item in items:
				metaInfoMap[str(item['nfid'])] = ast.literal_eval(item['metaInfo'])
			metaInfoMap = json.loads(json.dumps(metaInfoMap))
			return metaInfoMap
		else:
			return None

	def selectNilmMetaForUpdateInfo(self, sid, did, lfid):
		queryString = self.queryPool.getSelectNilmMetaForUpdateInfoQuery(sid, did, lfid)
		items = self._executeReadQuery(queryString)

		if items:
			metaInfoMap = {}
			for item in items:
				try:
					metaInfo = ast.literal_eval(str(item['metaInfo']))
					enabled = item['status']
					if enabled == 1:
						metaInfo['status'] = 'enabled'
					else:
						metaInfo['status'] = 'disabled'
					metaInfoMap[str(item['nfid'])] = metaInfo
				except Exception, e:
					self._logger.exception(e)
					self._logger.error(item)
			metaInfoMap = convertDictToString(metaInfoMap)
			return metaInfoMap
		else:
			return None

	def insertNilmMetaInfo(self, vfid, nfeederMetaInfo, metaVersion = '0.0.1'):
		queryString = self.queryPool.getInsertMetaInfoQuery(vfid, nfeederMetaInfo, metaVersion)
		self._executeWriteQuery(queryString)

	def updateNilmMetaInfo(self, vfid, vfidMeta, status):
		queryString = self.queryPool.getUpdateNilmMetaInfoQuery(vfid, vfidMeta, status)
		result = self._executeWriteQuery(queryString)
		return result

	def deleteNilmMetaInfo(self, sid):
		queryString = self.queryPool.getDeleteNilmMetaInfoQuery(sid)
		self._executeWriteQuery(queryString)



	# nilm_hourly_feeder_usage
	def insertNilmHourlyFeederUsage(self, date, usage, sid, did, lfid, nfid):
		queryString = self.queryPool.getInsertNilmHourlyFeederUsageQuery(date, usage, sid, did, lfid, nfid)
		self._executeWriteQuery(queryString)

	def deleteNilmHourlyFeederUsage(self, sid):
		queryString = self.queryPool.getDeleteNilmHourlyFeederUsageQuery(sid)
		self._executeWriteQuery(queryString)



	# nilm_daily_feeder_usage
	def insertNilmDailyFeederUsage(self, date, usage, sid, did, lfid, nfid):
		queryString = self.queryPool.getInsertNilmDailyFeederUsageQuery(date, usage, sid, did, lfid, nfid)
		self._executeWriteQuery(queryString)

	def deleteNilmDailyFeederUsage(self, sid):
		queryString = self.queryPool.getDeleteNilmDailyFeederUsageQuery(sid)
		self._executeWriteQuery(queryString)



	# appliance table
	def insertAppliance(self, sid, applianceType, name):
		queryString = self.queryPool.getInsertApplianceQuery(sid, applianceType, name)
		applianceId = self._executeWriteQuery(queryString)
		return applianceId

	def deleteAppliance(self, sid):
		queryString = self.queryPool.getDeleteApplianceQuery(sid)
		self._executeWriteQuery(queryString)



	# usage_router table
	def selectRegUsageRouter(self, sid):
		queryString = self.queryPool.getSelectRegUsageRouterQuery(sid)
		item = self._executeReadQuery(queryString)[0]
		if int(item['count']) == 0:
			return False
		return True

	def selectRegUsageRouterSids(self):
		queryString = self.queryPool.selectRegUsageRouterSidsQuery()
		nilmSids = []
		items = self._executeReadQuery(queryString)
		if items:
			for item in items:
				nilmSids.append(int(item['sid']))
		return nilmSids

	def insertRegUsageRouter(self, sid):
		if not self.selectRegUsageRouter(sid):
			queryString = self.queryPool.getInsertRegUsageRouterQuery(sid)
			self._executeWriteQuery(queryString)

	def deleteRegUsageRouter(self, sid):
		queryString = self.queryPool.getDeleteRegUsageRouterQuery(sid)
		self._executeWriteQuery(queryString)


	# nilm group table
	def selectNilmGroup(self, gids):
		queryString = self.queryPool.getSelecNilmGroupSidQuery(gids)
		nilmSids = []
		items = self._executeReadQuery(queryString)
		if items:
			for item in items:
				nilmSids.append(int(item['sid']))
		return nilmSids

	def selectNilmGroupMap(self, gids):
		queryString = self.queryPool.getSelecNilmGroupMapQuery(gids)
		nilmGroupMap = {}
		items = self._executeReadQuery(queryString)
		if items:
			for item in items:
				sid = int(item['sid'])
				gname = str(item['gname'])
				nilmGroupMap[sid] = gname
		return nilmGroupMap


	# created nilm sids
	def selectNotRegScheduledMetaStatusInfo(self, groupNames, sids, hasMeta, deadlineTS, forced):
		queryString = self.queryPool.getSelectMetaStatusQuery(groupNames, sids,hasMeta, deadlineTS, forced)
		metaStatusInfo = {}
		items = self._executeReadQuery(queryString)
		if items:
			for item in items:
				metaStatusInfo[int(item['sid'])] = int(item['did'])
		return metaStatusInfo

	def selectRegScheduledMetaStatusInfo(self, groupNames, sids, hasMeta, forced):
		queryString = self.queryPool.getSelectRegScheduledMetaStatusQuery(groupNames, sids, hasMeta, forced)
		metaStatusInfo = {}
		items = self._executeReadQuery(queryString)
		if items:
			for item in items:
				metaStatusInfo[int(item['sid'])] = int(item['did'])
		return metaStatusInfo

	# nilm site status
	def selectActiveNilmInfo(self, sids, forced=False):
		queryString = self.queryPool.getSelectActiveNilmInfo(sids, forced=forced)
		activeSiteInfo = {}
		items = self._executeReadQuery(queryString)
		if items:
			for item in items:
				activeSiteInfo[int(item['sid'])] = int(item['did'])
			return activeSiteInfo
		else:
			return None

	def selectCandidateNilmInfo(self, sids):
		queryString = self.queryPool.getSelectCandidateNilmInfo(sids)
		candidateSiteInfo = {}
		items = self._executeReadQuery(queryString)
		if items:
			for item in items:
				candidateSiteInfo[int(item['sid'])] = int(item['did'])
			return candidateSiteInfo
		else:
			return None

	def selectNilmCountryInfo(self, sids):
		queryString = self.queryPool.getSelectNilmCountryInfo(sids)
		nilmCountryDeviceMeta = getCountryDeviceMetaInfos()
		nilmCountryInfoMap = {}
		items = self._executeReadQuery(queryString)
		if items:
			for item in items:
				country = item['country']
				sid = int(item['sid'])
				timezone = item['TZ']
				deviceCh = int(item['deviceCh'])
				deviceFreq = int(item['deviceFreq'])
				nilmFreq = int(item['nilmFreq'])

				if not country:
					continue

				if not nilmCountryDeviceMeta.has_key(country) or not timezone in nilmCountryDeviceMeta[country]['timezone']:
					continue

				if nilmCountryInfoMap.has_key(country):
					nilmCountryInfoMap[country]['sids'].append(sid)
				else:
					nilmCountryInfoMap[country] = {
						'TZ': timezone,
						'sids': [sid],
						'deviceCh': deviceCh,
						'deviceFreq': deviceFreq,
						'nilmFreq': nilmFreq
					}
		return nilmCountryInfoMap

	def selectNilmUpdateTargetInfoForLoadBalance(self):
		queryString = self.queryPool.getNilmUpdateTargetInfoForLoadBalance()
		nilmLearnerTargetInfoForLoadBalance = {}
		items = self._executeReadQuery(queryString)
		if not items:
			return
		for item in items:
			sid = int(item['sid'])
			hasMeta = bool(item['hasMeta'])
			updateTS = int(item['updateTS'])
			sidUpdateTS = '%d_%s' %(updateTS, sid)
			if nilmLearnerTargetInfoForLoadBalance.has_key(hasMeta):
				nilmLearnerTargetInfoForLoadBalance[hasMeta].append(sidUpdateTS)
			else:
				nilmLearnerTargetInfoForLoadBalance[hasMeta] = [sidUpdateTS]
		return nilmLearnerTargetInfoForLoadBalance

	def updateScheduledLearingDateForLoadBalance(self, schedTS, sids):
		queryString = self.queryPool.getUpdateScheduledLearingDateForLoadBalance(schedTS, sids)
		self._executeWriteQuery(queryString)

	def updateNilmSiteStatus(self, sid, hasMeta = 1, updateTS = None):
		if not updateTS:
			updateTS = getUtcNowTS()
		queryString = self.queryPool.getUpdateNilmSiteStatusQuery(sid, updateTS, hasMeta)
		self._executeWriteQuery(queryString)


	def deleteNilmAllInfo(self, sid):
		updateTS = getUtcNowTS() - (16 * 24 * 60 * 60)
		queryStrings = []
		queryString.append(self.queryPool.getDeleteApplianceQuery(sid))
		queryString.append(self.queryPool.getDeleteNilmMetaInfoQuery(sid))
		queryString.append(self.queryPool.getDeleteRegUsageRouterQuery(sid))
		queryString.append(self.queryPool.getDeleteNilmVirtualFeederInfoQuery(sid))
		queryString.append(self.queryPool.getUpdateNilmSiteStatusQuery(sid, updateTS, 0))

		self._executeRemoveQuery(queryStrings)

	def selectVfidsQuery(self, sids):
		queryString = self.queryPool.getSelectVfidsQuery(sids)
		items = self._executeReadQuery(queryString)
		vfidMap = {}
		for item in items:
			vfidMap[int(item['vfid'])] = int(item['sid'])
		return vfidMap
