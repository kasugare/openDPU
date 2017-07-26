#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_datastore import getDBInfo
from common.conf_system import getSystemEnv
from RdsQueryPool import RdsQueryPool
import MySQLdb.cursors
import MySQLdb
import traceback
import time
import ast

class RdsHandler:
	def __init__(self, logger):
		self.queryPool = RdsQueryPool(logger)
		self._logger = logger

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

	def _executeBulkWriteQuery(self, queryString, dataSet):
		executionSize = 100
		start = 0
		end = executionSize
		dataSize = len(dataSet)
		try:
			dbConn = self._connectDb()
			dbCursor = dbConn.cursor()

			while True:
				self._logger.debug("- size of insert query : %d / %d" %(end, dataSize))
				slicedDataSet = dataSet[start:end]
				dbCursor.executemany(queryString, slicedDataSet)
				start = end
				end += executionSize
				if dataSize < end:
					break
			dbConn.commit()
			time.sleep(1)
		except Exception, e:
			self._logger.error(str(traceback.format_exc(e)))
		finally:
			if dbConn:
				dbConn.close()

	def _executeWriteQuery(self, queryString, dataSet):
		try:
			self._logger.info("# size of insert data set : %d" %len(dataSet))
			dbConn = self._connectDb()
			dbCursor = dbConn.cursor()
			insertCount = 0
			executionSize = 100

			for data in dataSet:
				dataQueryString = queryString %data
				dbCursor.execute(dataQueryString)
				if insertCount >= executionSize:
					dbConn.commit()
					insertCount = 0
					time.sleep(1)
				else:
					insertCount += 1
			dbConn.commit()
			if dbCursor.rowcount <= 0:
				return None
		except Exception, e:
			self._logger.exception(e)
		finally:
			if dbConn:
				dbConn.close()

	def _executeReadQuery(self, queryString):
		items = None
		try:
			dbConn = self._connectDb()
			dbCursor = dbConn.cursor()
			dbCursor.execute(queryString)

			if dbCursor.rowcount <= 0:
				return None
			items = dbCursor.fetchall()
		except Exception, e:
			self._logger.exception(e)
		finally:
			if dbConn:
				dbConn.close()
		return items

	# get activated nilm sids
	def selectGroupSiteMapQuery(self, gids):
		queryString = self.queryPool.getSelectGroupSidsQuery(gids)
		items = self._executeReadQuery(queryString)
		groupSiteMap = {}
		for item in items:
			groupSiteMap[int(item['sid'])] = int(item['did'])
		return groupSiteMap

	def selectRegistratedMetaSidsQuery(self):
		queryString = self.queryPool.getSelectRegistratedMetaSidsQuery()
		items = self._executeReadQuery(queryString)
		regMetaSidsMap = {}
		for item in items:
			regMetaSidsMap[int(item['sid'])] = {int(item['did']): int(item['regTS'])}
		return regMetaSidsMap

	def selectSiteStatusQuery(self):
		queryString = self.queryPool.getSelectSiteStatusQuery()
		items = self._executeReadQuery(queryString)
		nilmSiteStatusMap = {}
		for item in items:
			nilmSiteStatusMap[item['sid']] = item
		return nilmSiteStatusMap

	def selectSiteCountryCodeQuery(self, sids):
		queryString = self.queryPool.getSelectSiteCountryCodeQuery(sids)
		items = self._executeReadQuery(queryString)
		siteCountryCodeInfoMap = {}
		for item in items:
			siteCountryCodeInfoMap[int(item['sid'])] = item['country']
		return siteCountryCodeInfoMap

	def insertNilmStatusQuery(self, nilmStatusSet):
		queryString = self.queryPool.getInsertNilmStatusQuery()
		try:
			self._executeWriteQuery(queryString, nilmStatusSet)
		except Exception, e:
			self._logger.exception(e)
