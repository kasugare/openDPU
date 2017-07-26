#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_datetime import getUtcNowTS, convertOneDayBaseTimestamp

class RdsQueryPool:
	def __init__(self, logger):
		self._logger = logger

	def _printLog(self, queryString):
		queryString = " ".join(queryString.split())
		self._logger.debug(" - %s" %queryString)

	def _addWhere(queryString, columnName, whereValues):
		values = ",".join([str(value) for value in whereValues])
		whereQueryString = "%s AND %s in (%s)" %(queryString, columnName, values)
		return whereQueryString


	# device table
	def getSelectEdmTypeQuery(self, sid):
		queryString = """SELECT model AS inst, serialNumber AS sn FROM device
			WHERE site_id in (%d)""" %(sid)
		self._printLog(queryString)
		return queryString


	# nilm_virtual_feeder table
	def getCheckNilmVirtualFeederQuery(self, sid, did, lfid, nfid):
		queryString = """SELECT id AS vfid, count(*) AS count FROM nilm_virtual_feeder
			WHERE siteId = %d
				AND deviceId = %d
				AND localFeederId = %d
				AND nilmFeederId = %d GROUP BY id""" %(sid, did, lfid, nfid)
		self._printLog(queryString)
		return queryString

	def getSelectNilmVirtualFeederQuery(self, sid, did, lfid):
		queryString = """SELECT B.nilmFeederId AS nfid, A.metaInfo FROM nilm_virtual_feeder
			WHERE A.virtualFeederId = B.id
				AND B.siteId = %d
				AND B.deviceId = %d
				AND B.localFeederId = %d
			ORDER BY B.nilmFeederId ASC;""" %(sid, did, lfid)
		self._printLog(queryString)
		return queryString

	def getSelectApplianceIdsQuery(self, sid):
		queryString = """SELECT applianceId FROM nilm_virtual_feeder
			WHERE siteId = %d""" %(sid)
		self._printLog(queryString)
		return queryString

	def getSelectNilmVirtualFeederIdQuery(self, sid, did, lfid, nfid):
		queryString = """SELECT id AS vfid FROM nilm_virtual_feeder
			WHERE siteId = %d
				AND deviceId = %d
				AND localFeederId = %d
				AND nilmFeederId = %d""" %(sid, did, lfid, nfid)
		self._printLog(queryString)
		return queryString

	def getSelectNilmVirtualFeederMap(self, sid, did, lfid):
		queryString = """SELECT nilmFeederId as nfid, id as vfid
			FROM nilm_virtual_feeder
			WHERE siteId = %d
				AND deviceId = %d
				AND localFeederId = %d""" %(sid, did, lfid)
		self._printLog(queryString)
		return queryString

	def getSelectNilmSitesVirtualFeederMap(self, sids):
		strSids = ",".join(["%d" %sid for sid in sids])
		queryString = """SELECT siteId as sid
				, nilmFeederId as nfid
				, id as vfid
			FROM nilm_virtual_feeder
			WHERE siteId in (%s)
				AND localFeederId = 3""" %(strSids)
		self._printLog(queryString)
		return queryString

	def getInsertNilmVirtualFeederInfoQuery(self, sid, did, lfid, nfid, applianceId):
		queryString = """INSERT INTO nilm_virtual_feeder (siteId, deviceId, localFeederId, nilmFeederId, applianceId)
			VALUES (%d, %d, %d, %d, %d)""" %(sid, did, lfid, nfid, applianceId)
		self._printLog(queryString)
		return queryString

	def getUpdateNilmVirtualFeederForApplianceDeleteQuery(self, sid):
		queryString = """UPDATE nilm_virtual_feeder
			SET applianceId = NULL
				WHERE siteId = %s""" %(sid)
		self._printLog(queryString)
		return queryString


	def getDeleteNilmVirtualFeederInfoQuery(self, sid):
		queryString = """DELETE FROM nilm_virtual_feeder
			WHERE siteId = %d""" %(sid)
		self._printLog(queryString)
		return queryString



	# nilm_meta table
	def getCheckNilmMetaInfQuery(self, sid, did, lfid, nfid):
		queryString = """SELECT id AS metaId, count(*) AS count FROM nilm_meta
			WHERE virtualFeederId = %d GROUP BY id""" %(vfid)
		self._printLog(queryString)
		return queryString

	def getSelectNilmMetaInfoQuery(self, sid, did, lfid):
		queryString = """SELECT B.nilmFeederId AS nfid, A.metaInfo FROM nilm_meta A, nilm_virtual_feeder B
			WHERE A.virtualFeederId = B.id
				AND B.siteId = %d
				AND B.deviceId = %d
				AND B.localFeederId = %d
				AND B.nilmFeederId != 999
				AND A.isEnabled = 1
			ORDER BY B.localFeederId, B.nilmFeederId ASC;""" %(sid, did, lfid)
		self._printLog(queryString)
		return queryString

	def getSelectNilmMetaForUpdateInfoQuery(self, sid, did, lfid):
		queryString = """SELECT B.nilmFeederId AS nfid, A.metaInfo, A.isEnabled as status FROM nilm_meta A, nilm_virtual_feeder B
			WHERE A.virtualFeederId = B.id
				AND B.siteId = %d
				AND B.deviceId = %d
				AND B.localFeederId = %d
				AND B.nilmFeederId != 999
			ORDER BY B.localFeederId, B.nilmFeederId ASC;""" %(sid, did, lfid)
		self._printLog(queryString)
		return queryString

	def getInsertMetaInfoQuery(self, vfid, nfeederMetaInfo, metaVersion):
		queryString = """INSERT INTO nilm_meta (virtualFeederId, metaInfo, isEnabled, version)
			VALUES (%d, "%s", 1, "%s")""" %(vfid, str(nfeederMetaInfo), metaVersion)
		self._printLog(queryString)
		return queryString

	def getUpdateNilmMetaInfoQuery(self, vfid, vfidMeta, status):
		updateTS = getUtcNowTS()
		queryString = """UPDATE nilm_meta
			SET metaInfo = "%s",
				isEnabled = %d,
				updateDate = FROM_UNIXTIME('%d')
			WHERE virtualFeederId = %d""" %(vfidMeta, status, updateTS, vfid)
		self._printLog(queryString)
		return queryString

	def getDeleteNilmMetaInfoQuery(self, sid):
		queryString = """DELETE FROM nilm_meta
			WHERE virtualFeederId in (SELECT * FROM (
					SELECT id FROM nilm_virtual_feeder
						WHERE siteId = %d
				) A )""" %(sid)
		self._printLog(queryString)
		return queryString


	# nilm_hourly_feeder_usage
	def getInsertNilmHourlyFeederUsageQuery(self, date, usage, sid, did, lfid, nfid):
		queryString = """INSERT INTO nilm_hourly_feeder_usage (date, virtualFeederId, unitPeriodUsage)
			SELECT '%s', id, %d FROM nilm_virtual_feeder
				WHERE siteId = %d
					AND deviceId = %d
					AND localFeederId = %d
					AND nilmFeederId = %d
			ON DUPLICATE KEY UPDATE unitPeriodUsage = %f""" %(date, usage, sid, did, lfid, nfid, usage)
		self._printLog(queryString)
		return queryString

	def getDeleteNilmHourlyFeederUsageQuery(self, sid):
		queryString = """DELETE FROM nilm_hourly_feeder_usage
				WHERE virtualFeederId in (SELECT * FROM (
					SELECT id FROM nilm_virtual_feeder
						WHERE siteId = %d
				) A )""" %(sid)
		self._printLog(queryString)
		return queryString


	# nilm_daily_feeder_usage
	def getInsertNilmDailyFeederUsageQuery(self, date, usage, sid, did, lfid, nfid):
		queryString = """INSERT INTO nilm_daily_feeder_usage (date, virtualFeederId, unitPeriodUsage)
			SELECT '%s', id, %d FROM nilm_virtual_feeder
				WHERE siteId = %d
					AND deviceId = %d
					AND localFeederId = %d
					AND nilmFeederId = %d
			ON DUPLICATE KEY UPDATE unitPeriodUsage = %f""" %(date, usage, sid, did, lfid, nfid, usage)
		self._printLog(queryString)
		return queryString

	def getDeleteNilmDailyFeederUsageQuery(self, sid):
		queryString = """DELETE FROM nilm_daily_feeder_usage
				WHERE virtualFeederId in (SELECT * FROM (
					SELECT id FROM nilm_virtual_feeder
						WHERE siteId = %d
				) A )""" %(sid)
		self._printLog(queryString)
		return queryString


	# appliance table
	def getInsertApplianceQuery(self, sid, applianceType, name):
		queryString = """INSERT INTO appliance (site_id, appliance_type_id, name)
			VALUES (%d, %d, "%s")""" %(sid, applianceType, name)
		self._printLog(queryString)
		return queryString

	def getDeleteApplianceQuery(self, sid):
		queryString = """ DELETE FROM appliance
			WHERE id in (SELECT * FROM (
				SELECT B.id FROM nilm_virtual_feeder A, appliance B
					WHERE A.applianceId = B.id
						AND A.siteId = %d
				) C )"""%(sid)

		self._printLog(queryString)
		return queryString


	# usage_router table
	def getSelectRegUsageRouterQuery(self, sid):
		queryString = """SELECT count(siteId) AS count FROM usage_router
			WHERE siteId = %d""" %(sid)
		self._printLog(queryString)
		return queryString

	def selectRegUsageRouterSidsQuery(self):
		queryString = """SELECT siteId AS sid FROM usage_router"""
		self._printLog(queryString)
		return queryString

	def getInsertRegUsageRouterQuery(self, sid):
		queryString = """INSERT INTO usage_router (siteId, isNilm)
			VALUES (%d, %d)""" %(sid, 1)
		self._printLog(queryString)
		return queryString

	def getDeleteRegUsageRouterQuery(self, sid):
		queryString = """DELETE FROM usage_router
			WHERE siteId = %d""" %(sid)
		self._printLog(queryString)
		return queryString


	# nilm group table
	def getSelecNilmGroupSidQuery(self, gids):
		gids = ",".join(["""%d""" %groupId for groupId in gids])
		queryString = """SELECT A.siteId as sid FROM group_site A, device B
			WHERE A.siteId = B.site_id
				AND A.groupId in (%s)""" %(gids)
		self._printLog(queryString)
		return queryString

	def getSelecNilmGroupMapQuery(self, gids):
		gids = ",".join(["""%d""" %groupId for groupId in gids])
		queryString = """SELECT A.siteId as sid
				, A.groupName as gname
			FROM group_site A, device B
			WHERE A.siteId = B.site_id
				AND A.groupId in (%s)""" %(gids)
		self._printLog(queryString)
		return queryString


	# created nilm sids
	def getSelectMetaStatusQuery(self, gids, sids, hasMeta, deadlineTS, forced):
		gids = ",".join(["""%d""" %groupId for groupId in gids])
		sids = ','.join([str(sid) for sid in sids])
		queryString = """SELECT A.siteId as sid
				, A.deviceId as did
			FROM nilm_site_status A, group_site B
			WHERE A.siteId = B.siteId
				AND A.deviceStatus = 'active'
				AND A.nilmStatus = 'active'
				AND B.groupId in (%s)
				AND A.siteId in (%s)
				AND A.hasMeta = '%d'""" %(gids, sids, hasMeta)
		if not forced:
			queryString = """%s 
				AND A.scheduledDate is null
				AND A.updateDate <= FROM_UNIXTIME('%d')""" %(queryString, deadlineTS)
		# self._printLog(queryString)
		return queryString

	def getSelectRegScheduledMetaStatusQuery(self, gids, sids, hasMeta, forced):
		gids = ",".join(["""%d""" %groupId for groupId in gids])
		sids = ','.join([str(sid) for sid in sids])
		currentBTS = convertOneDayBaseTimestamp(getUtcNowTS()*1000)/1000
		queryString = """SELECT A.siteId as sid
				, A.deviceId as did
			FROM nilm_site_status A, group_site B
			WHERE A.siteId = B.siteId
				AND A.deviceStatus = 'active'
				AND A.nilmStatus = 'active'
				AND B.groupId in (%s)
				AND A.siteId in (%s)
				AND A.hasMeta = '%d'""" %(gids, sids, hasMeta)
		if not forced:
			queryString = """%s 
				AND A.updateDate <= A.scheduledDate
				AND A.scheduledDate = FROM_UNIXTIME(%d)""" %(queryString, currentBTS)
		# self._printLog(queryString)
		return queryString

	# nilm site status
	def getSelectActiveNilmInfo(self, sids, forced=False):
		sids = ','.join([str(sid) for sid in sids])
		queryString = """SELECT siteId as sid,
				deviceId as did
			FROM nilm_site_status
			WHERE siteId in (%s)
				AND nilmStatus = 'active'
				AND hasMeta = 1""" %(sids)
		if not forced:
			queryString = """%s
				AND deviceStatus = 'active'""" %(queryString)
		# self._printLog(queryString)
		return queryString

	def getSelectCandidateNilmInfo(self, sids):
		sids = ','.join([str(sid) for sid in sids])
		queryString = """SELECT siteId as sid,
				deviceId as did
			FROM nilm_site_status
			WHERE siteId in (%s)
				AND nilmStatus = 'active'
				AND hasMeta = 0""" %(sids)
		# self._printLog(queryString)
		return queryString

	def getSelectNilmCountryInfo(self, sids):
		sids = ','.join([str(sid) for sid in sids])
		queryString = """SELECT A.siteId as sid,
				A.deviceId as did,
				B.country,
				B.timezone as TZ,
				A.deviceCh,
				A.deviceFreq,
				A.nilmFreq
			FROM nilm_site_status A,
				site B 
			WHERE A.siteId = B.id
				AND B.appId not in (29)
				AND B.id in (%s)""" %(sids)
		# self._printLog(queryString)
		return queryString

	def getNilmUpdateTargetInfoForLoadBalance(self):
		queryString = """SELECT siteId as sid,
				hasMeta,
				(UNIX_TIMESTAMP(regDate) * 1000) as regTS,
				(UNIX_TIMESTAMP(updateDate) * 1000) as updateTS
			FROM nilm_site_status
			WHERE deviceStatus = 'active'
				AND nilmstatus = 'active'
			ORDER BY updateDate ASC"""
		# self._printLog(queryString)
		return queryString

	def getUpdateScheduledLearingDateForLoadBalance(self, schedTS, sids):
		queryString = """UPDATE nilm_site_status
			SET scheduledDate = FROM_UNIXTIME(%d)
			WHERE siteId in (%s)""" %(schedTS, sids)
		# self._printLog(queryString)
		return queryString


	def getUpdateNilmSiteStatusQuery(self, sid, updateTS=None, hasMeta=0):
		if not updateTS:
			updateTS = getUtcNowTS()
		queryString = """UPDATE nilm_site_status
			SET hasMeta = %d,
				updateDate = FROM_UNIXTIME(%d)
			WHERE siteId = %s""" %(hasMeta, updateTS, sid)
		self._printLog(queryString)
		return queryString

	def getSelectVfidsQuery(self, sids):
		strSids = ','.join(["""'%s'""" %str(sid) for sid in sids])
		queryString = """SELECT A.siteId as sid
				, A.id as vfid 
			FROM nilm_virtual_feeder A
				, appliance B
				, nilm_meta C
				, nilm_site_status D
			WHERE A.applianceId = B.id 
				AND A.id = C.virtualFeederId 
				AND B.appliance_type_id in (69, 70, 71)
				AND A.siteId = D.siteId
				AND D.deviceStatus = 'active'
				AND C.isEnabled = true 
				AND A.siteId in (%s)""" %(strSids)
		self._printLog(queryString)
		return queryString
