#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_system import getNilmMetaSearchPeriod, getNilmMetaUpdatePeriod
from common.util_datetime import getUtcNowTS
from data_store.etable.ETableHandler import ETableHandler
from data_store.rds.RdsHandler import RdsHandler

class DataServiceManager:
	def __init__(self, logger):
		self._logger = logger
		self._dbHandler = RdsHandler(self._logger)
		self._etableHandler = ETableHandler(self._logger)

	def getNilmMetaInfo(self, sid, did, lfid):
		self._logger.info("# get nilm meta info")
		metaInfo = self._dbHandler.selectNilmMetaInfo(sid, did, lfid)
		return metaInfo

	def getNilmMetaForUpdateInfo(self, sid, did, lfid):
		self._logger.info("# get nilm meta for update info")
		metaInfo = self._dbHandler.selectNilmMetaForUpdateInfo(sid, did, lfid)
		return metaInfo

	def getEdmType(self, sid):
		self._logger.info("# get edm types")
		edmType = self._dbHandler.selectEdmTypeQuery(sid)
		self._logger.debug("edm device types : %s" %(str(edmType)))
		return edmType

	def getRemovableDataService(self, sid):
		self._logger.info("# remove nilm infos")
		return self._dbHandler.selectRegUsageRouter(sid)

	def getNilmVirtualFeederId(self, sid, did, lfid, nfid):
		self._logger.debug("# get nilm virtual feeder ids")
		vfid = self._dbHandler.selectNilmVirtualFeederId(sid, did, lfid, nfid)
		return vfid

	def selectRegUsageRouterSids(self):
		self._logger.info("# get registered nilm sids in router table")
		sids = self._dbHandler.selectRegUsageRouterSids()
		self._logger.debug("- sids : %s" %str(sids))
		return sids

	def getNilmTargetSids(self, gids):
		self._logger.info("# get nilm target group sids")
		sids = self._dbHandler.selectNilmGroup(gids)
		# self._logger.debug("- sids : %s" %str(sids))
		return sids

	def getNilmTargetGroupMap(self, gids):
		self._logger.info("# get nilm target group name - site id Map")
		nilmGroupMap = self._dbHandler.selectNilmGroupMap(gids)
		# self._logger.debug("- sids : %s" %str(sids))
		return nilmGroupMap

	def getNilmMetaSearchInfo(self, groupNames, sids, startTS, forced, hasMeta):
		self._logger.info("# get nilm meta search info")
		deadlineTS = startTS / 1000
		metaSearchTargetInfo = self._dbHandler.selectNotRegScheduledMetaStatusInfo(groupNames, sids, hasMeta, deadlineTS, forced)
		if not forced:
			regScheduledMetaSearchTargetInfo = self._dbHandler.selectRegScheduledMetaStatusInfo(groupNames, sids, hasMeta, forced)
			for sid in regScheduledMetaSearchTargetInfo.keys():
				did = regScheduledMetaSearchTargetInfo[sid]
				metaSearchTargetInfo[sid] = did
		return metaSearchTargetInfo

	def getNilmMetaUpdateSearchInfo(self, groupNames, sids, startTS, forced, hasMeta):
		self._logger.info("# get nilm meta update search info")
		deadlineTS = startTS / 1000
		metaSearchTargetInfo = self._dbHandler.selectRegScheduledMetaStatusInfo(groupNames, sids, hasMeta, forced)
		return metaSearchTargetInfo

	def getActiveNilmInfo(self, sids, forced=False):
		self._logger.info("# get activated nilm sids info")
		activeSiteMap = self._dbHandler.selectActiveNilmInfo(sids, forced=forced)
		return activeSiteMap

	def getCandidateNilmInfo(self, sids):
		self._logger.info("# get candidate nilm sids info")
		candidateSiteMap = self._dbHandler.selectCandidateNilmInfo(sids)
		return candidateSiteMap

	def getNilmCountryInfo(self, sids):
		self._logger.info("# get nilm site status info")
		nilmCountryInfoMap = self._dbHandler.selectNilmCountryInfo(sids)
		return nilmCountryInfoMap

	def getNilmMetaUpdateTargetInfoForLoadBalance(self):
		self._logger.info("# get nilm meta update target info for load balance")
		nilmLearnerTargetInfoForLoadBalance = self._dbHandler.selectNilmUpdateTargetInfoForLoadBalance()
		return nilmLearnerTargetInfoForLoadBalance

	def setScheduledLearingDateForLoadBalance(self, schedLearnerMap):
		self._logger.info("# set nilm meta learning target info for load balance")
		for schedTS in schedLearnerMap.keys():
			sids = ','.join([str(sid) for sid in schedLearnerMap[schedTS]])
			if sids:
				self._dbHandler.updateScheduledLearingDateForLoadBalance(schedTS/1000, sids)

	def getNilmSitesVirtualFeederMap(self, sids):
		siteVfidMap = self._dbHandler.selectNilmSitesVirtualFeederMap(sids)
		return siteVfidMap

	def setNilmFeederUsage(self, qhourlyDataSet, hourlyDataSet, dailyDataSet):
		self._logger.info("# set nilm data info")
		
		self._etableHandler.insertNilmQHourlyFeederUsage(qhourlyDataSet)
		self._etableHandler.insertNilmHourlyFeederUsage(hourlyDataSet)
		self._etableHandler.insertNilmDailyFeederUsage(dailyDataSet)

	def setNilmQHourlyFeederUsage(self, startTS, sid, did, lfid, qHourlyUsage, qHourlyAppOn):
		self._logger.info("# set nilm hourly feeder usage info")
		qHourTS = 900000
		mwhUnit = 1000
		arrangedUsageDataSet = []

		def isNumber(string):
			try:
				int(string)
				return True
			except ValueError, e:
				self._logger.exception(e)
				return False
		vfidMappingTable = self._dbHandler.selectNilmVirtualFeederMap(sid, did, lfid)
		for nfid in qHourlyUsage.keys():
			if vfidMappingTable.has_key(int(nfid)):
				for qHour in qHourlyUsage[nfid]:
					if isNumber(qHour):
						vfid = vfidMappingTable[int(nfid)]
						qHourlyUsageTS = startTS + (qHourTS * int(qHour))
						nfidUsage = int(qHourlyUsage[nfid][qHour] * mwhUnit)
						if qHourlyAppOn[nfid][qHour] == 1:
							appOn = True
						else:
							appOn = False
						if int(nfid) == 999 and nfidUsage < 0:
							nfidUsage = 0
						arrangedUsageDataSet.append({'date': qHourlyUsageTS, 'unitperiodusage': nfidUsage, 'virtualfeederid': vfid, 'poweron': appOn})
					else:
						self._logger.warn('- wrong string qHour : %s' %(qHour))
						self._logger.warn('- qHourly usage : %s' %(str(qHourlyUsage)))
						self._logger.warn('- qHourly appliance on/off : %s' %(str(qHourlyAppOn)))
			else:
				self._logger.warn('- not exist vfid, sid : %d, nfid : %s' %(sid, nfid))

		self._etableHandler.insertNilmQHourlyFeederUsage(arrangedUsageDataSet)

	def setNilmHourlyFeederUsage(self, startTS, sid, did, lfid, hourlyUsage, hourlyAppOn):
		self._logger.info("# set nilm hourly feeder usage info")
		hourTS = 3600000
		mwhUnit = 1000
		arrangedUsageDataSet = []
		def isNumber(string):
			try:
				int(string)
				return True
			except ValueError, e:
				self._logger.exception(e)
				return False
		vfidMappingTable = self._dbHandler.selectNilmVirtualFeederMap(sid, did, lfid)
		for nfid in hourlyUsage.keys():
			if vfidMappingTable.has_key(int(nfid)):
				for hour in hourlyUsage[nfid]:
					if isNumber(hour):
						vfid = vfidMappingTable[int(nfid)]
						hourlyUsageTS = startTS + (hourTS * int(hour))
						nfidUsage = int(hourlyUsage[nfid][hour] * mwhUnit)
						if hourlyAppOn[nfid][hour] == 1:
							appOn = True
						else:
							appOn = False
						if int(nfid) == 999 and nfidUsage < 0:
							nfidUsage = 0
						arrangedUsageDataSet.append({'date': hourlyUsageTS, 'unitperiodusage': nfidUsage, 'virtualfeederid': vfid, 'poweron': appOn})
					else:
						self._logger.warn('- wrong string hour : %s' %(hour))
						self._logger.warn('- hourly usage : %s' %(str(hourlyUsage)))
						self._logger.warn('- hourly appliance on/off : %s' %(str(hourlyAppOn)))
			else:
				self._logger.warn('- not exist vfid, sid : %d, nfid : %s' %(sid, nfid))

		self._etableHandler.insertNilmHourlyFeederUsage(arrangedUsageDataSet)


	def setNilmDailyFeederUsage(self, dailyBaseTS, sid, did, lfid, dailyUsage, dailyAppOn, dailyCount):
		self._logger.info("# set nilm daily feeder usage info")
		mwhUnit = 1000
		arrangedUsageDataSet = []
		vfidMappingTable = self._dbHandler.selectNilmVirtualFeederMap(sid, did, lfid)

		for nfid in dailyUsage.keys():
			if vfidMappingTable.has_key(int(nfid)):
				vfid = vfidMappingTable[int(nfid)]
				nfidUsage = int(dailyUsage[nfid] * mwhUnit)
				nfidDailyCount = dailyCount[nfid]
				if dailyAppOn[nfid] == 1:
					appOn = True
				else:
					appOn = False
				if int(nfid) == 999 and nfidUsage < 0:
					nfidUsage = 0
				arrangedUsageDataSet.append({'date': dailyBaseTS, 'unitperiodusage': nfidUsage, 'virtualfeederid': vfid, 'poweron': appOn, 'usagecount': nfidDailyCount})
			else:
				self._logger.warn('- not exist vfid, sid : %d, nfid : %s' %(sid, nfid))
		self._etableHandler.insertNilmDailyFeederUsage(arrangedUsageDataSet)

	def setNilmMetaSearchInfo(self, nilmMetaInfo):
		sid = nilmMetaInfo['sid']
		did = nilmMetaInfo['did']
		lfid = nilmMetaInfo['lfid']
		nfidMetaSearchInfo = nilmMetaInfo['metaSearch']

		self._dbHandler.insertRegUsageRouter(sid)
		self._dbHandler.updateNilmSiteStatus(sid, 1)

		for nfid in nfidMetaSearchInfo.keys():
			appType = nfidMetaSearchInfo[nfid]['appType']
			vfidMeta = nfidMetaSearchInfo[nfid]['vfidMeta']
			self._logger.info("# save new meta in rds, sid : %d, did : %d, lfid : %d, nfid : %d" %(sid, did, lfid, nfid))
			if vfidMeta:
				vfid = self._dbHandler.insertNilmVirtualFeederInfo(sid, did, lfid, nfid, appType)
				if vfidMeta.has_key('meta-version'):
					self._dbHandler.insertNilmMetaInfo(vfid, vfidMeta, str(vfidMeta['meta-version']))
				elif vfidMeta.has_key('meta_version'):
					self._dbHandler.insertNilmMetaInfo(vfid, vfidMeta, str(vfidMeta['meta_version']))
				else:
					self._dbHandler.insertNilmMetaInfo(vfid, vfidMeta)
			else:
				self._dbHandler.insertNilmVirtualFeederInfo(sid, did, lfid, nfid, appType)

	def setNilmMetaUpdateInfo(self, nilmMetaInfo):
		sid = nilmMetaInfo['sid']
		did = nilmMetaInfo['did']
		lfid = nilmMetaInfo['lfid']
		nfidMetaSearchInfo = nilmMetaInfo['metaSearch']

		self._dbHandler.updateNilmSiteStatus(sid, 1)

		for nfid in nfidMetaSearchInfo.keys():
			appType = nfidMetaSearchInfo[nfid]['appType']
			vfidMeta = nfidMetaSearchInfo[nfid]['vfidMeta']
			self._logger.info("# update new meta in rds, sid : %d, did : %d, lfid : %d, nfid : %d" %(sid, did, lfid, nfid))
			if vfidMeta:
				vfid = self._dbHandler.insertNilmVirtualFeederInfo(sid, did, lfid, nfid, appType)
				if vfidMeta.has_key('meta-version'):
					self._dbHandler.insertNilmMetaInfo(vfid, vfidMeta, str(vfidMeta['meta-version']))
				elif vfidMeta.has_key('meta_version'):
					self._dbHandler.insertNilmMetaInfo(vfid, vfidMeta, str(vfidMeta['meta_version']))
				else:
					self._dbHandler.insertNilmMetaInfo(vfid, vfidMeta)
			else:
				self._dbHandler.insertNilmVirtualFeederInfo(sid, did, lfid, nfid, appType)

		if nilmMetaInfo.has_key('metaUpdate'):
			nfidMetaUpdateInfo = nilmMetaInfo['metaUpdate']
			vfidMappingTable = self._dbHandler.selectNilmVirtualFeederMap(sid, did, lfid)
			for nfid in nfidMetaUpdateInfo.keys():
				if vfidMappingTable.has_key(nfid):
					vfid = vfidMappingTable[nfid]
					vfidMeta = nfidMetaUpdateInfo[nfid]['vfidMeta']
					status = nfidMetaUpdateInfo[nfid]['status']
					self._dbHandler.updateNilmMetaInfo(vfid, vfidMeta, status)

	def setUsageRouteDataService(self, sid):
		self._logger.info("# set nilm route info")
		try:
			self._dbHandler.insertRegUsageRouter(sid)
		except Exception, e:
			self._logger.exception(e)

	def setNilmVirtualFeederInfo(self, sid, did, lfid, nfid, insertAppliance = 71):
		self._logger.info("# set nilm virtual feeder info, sid : %d, did : %d, lfid : %d, nfid : %d, appliance type : %d" %(sid, did, lfid, nfid, insertAppliance))
		vfid = self._dbHandler.insertNilmVirtualFeederInfo(sid, did, lfid, nfid, insertAppliance)
		return vfid

	def setNilmMetaInfo(self, vfid, newAppMetaList):
		self._dbHandler.insertNilmMetaInfo(vfid, newAppMetaList)

	def removeDataService(self, sid, delTypes):
		self._logger.info("# remove meta info, delete types : %s" %str(delTypes))
		for delType in delTypes:
			if delType == 'vfeeder':
				updateTS = getUtcNowTS() - (16 * 24 * 60 * 60)
				self._dbHandler.deleteAppliance(sid)
				self._dbHandler.deleteNilmMetaInfo(sid)
				self._dbHandler.deleteRegUsageRouter(sid)
				self._dbHandler.deleteNilmVirtualFeederInfo(sid)
				self._dbHandler.updateNilmSiteStatus(sid, 0, updateTS)

			# elif delType == 'appliance':
			# 	self._dbHandler.deleteAppliance(sid)
			# 	self._dbHandler.updateNilmVirtualFeederForApplianceDelete(sid)
			# elif delType == 'meta':
			# 	self._dbHandler.deleteNilmMetaInfo(sid)
			# elif delType == 'router':
			# 	self._dbHandler.deleteRegUsageRouter(sid)

	def getNilmVfidsMap(self, sids):
		vfidMap = self._dbHandler.selectVfidsQuery(sids)
		return vfidMap

	def getNilmDailyEnabledMap(self, dailyTSs, vfids):
		vfidsEnabledMap = self._etableHandler.selectNilmDailyEnabledMap(dailyTSs, vfids)
		return vfidsEnabledMap

