#!/usr/bin/env python
# -*- coding: utf-8 -*-

class RdsQueryPool:
	def __init__(self, logger):
		self._logger = logger

	def _printLog(self, queryString):
		queryString = " ".join(queryString.split())
		# self._logger.debug(" - %s" %queryString)

	# select
	def getSelectGroupSidsQuery(self, gids):
		strGids = ','.join(["""'%s'""" %str(gid) for gid in gids])
		queryString = """SELECT A.siteId AS sid
				, B.id AS did
				, UNIX_TIMESTAMP(B.createdAt) as dts
			FROM group_site A, device B
			WHERE A.siteId = B.site_id
				AND A.groupId in (%s)""" %(strGids)
		self._printLog(queryString)
		return queryString

	def getSelectRegistratedMetaSidsQuery(self):
		queryString = """SELECT B.siteId AS sid
				, B.deviceId as did
				, UNIX_TIMESTAMP(B.regDate) as regTS
			FROM nilm_meta A, nilm_virtual_feeder B
			WHERE A.virtualFeederId = B.id
			GROUP BY sid"""
		self._printLog(queryString)
		return queryString

	def getSelectSiteStatusQuery(self):
		queryString = """SELECT siteId as sid
				, deviceId as did
				, deviceCh
				, deviceFreq
				, nilmFreq
				, deviceStatus
				, nilmStatus
				, hasMeta
				, UNIX_TIMESTAMP(regDate) as regTS
				, UNIX_TIMESTAMP(updateDate) as updateTS
			FROM nilm_site_status"""
		self._printLog(queryString)
		return queryString

	def getSelectSiteCountryCodeQuery(self, sids):
		strSids = ','.join([str(int(sid)) for sid in sids])
		queryString = """SELECT id as sid, country
			FROM site
			WHERE id in (%s)""" %(strSids)
		self._printLog(queryString)
		return queryString

	# insert
	def getInsertNilmStatusQuery(self):
		queryString = """INSERT INTO nilm_site_status (siteId, deviceId, deviceCh, deviceFreq, nilmFreq, deviceStatus, nilmStatus, hasMeta, regDate, updateDate) VALUES(%d, %d, %d, %d, %d, '%s', '%s', %d, FROM_UNIXTIME(%d), FROM_UNIXTIME(%d))
			ON DUPLICATE KEY UPDATE
				deviceStatus = VALUES(deviceStatus)
				, nilmStatus = VALUES(nilmStatus)
				, hasMeta = VALUES(hasMeta)
				, updateDate = VALUES(updateDate)"""
		self._printLog(queryString)
		return queryString
