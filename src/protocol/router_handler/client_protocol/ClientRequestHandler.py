#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_version import getVersionInfo
from common.conf_nilm_groups import getNilmGroupTable, getNilmGroupInfoForClient
from common.util_common import cvtWorkerId as cvtClientId
from protocol.message_pool.MessageGenerator import genReqJobCompleted, genResOK, genReqError, genReqNilmRawData, genResVersion, genResNilmGroupInfo, genResWorkersResources, genResNilmOutputCheckInfo
from nilm_master.job_container.nilm_output_checker.NilmOutputChecker import NilmOutputChecker
from data_store.DataServiceManager import DataServiceManager
from tcp_modules.NetworkHandler import DataSender
import pickle
import copy
import sys

class ClientRequestHandler:
	def __init__(self, logger, socketObj, resourceManager, jobQ):
		self._logger = logger
		self._jobQ = jobQ
		self._resourceManager = resourceManager
		self._clientId = cvtClientId(socketObj)
		self._dataManager = DataServiceManager(logger)
		self._sender = DataSender(logger, socketObj)

	def doNilmJobOption(self, message):
		try:
			orderSheet = message['orderSheet']
			self._sendMessage(genResOK())
			orderSheet['jobs'] = self._checkJobTypes(orderSheet)
			orderSheet['gids'] = self._checkGroups(orderSheet['groupNames'])
			orderSheet['sids'] = self._checkSids(orderSheet)
			if orderSheet['remove']:
				self._delNilmInfo(orderSheet)
			else:
				self._jobQ.put_nowait(pickle.dumps(genReqNilmRawData(orderSheet)))
		except Exception, e:
			self._logger.exception(e)
			self._logger.warn("# wrong user params : check job types")
			self._sendMessage(genReqError("check your job types : it allowed params as app_usage, meta_search and meta_update."))
			self._closeConnection()

	def checkClusterVersion(self, message):
		orderSheet = message['orderSheet']
		if orderSheet.has_key('version'):
			versionInfo = getVersionInfo()
			self._sendMessage(genResVersion(versionInfo))

	def getWorkerResources(self, message):
		orderSheet = message['orderSheet']
		if not orderSheet.has_key('workers'):
			self._sendMessage(genReqError("# Workers are not activate"))
			self._sendMessage(genReqError(e))
			return

		workers = orderSheet['workers']
		if 'all' in workers:
			try:
				workerResourcesMessage = self._getWorkerResoures()
				self._sendMessage(genResWorkersResources(workerResourcesMessage))
			except Exception, e:
				self._logger.exception(e)
				self._sendMessage(genReqError(e))

	def getNilmGroupInfo(self, message):
		orderSheet = message['orderSheet']
		if orderSheet.has_key('nilmGroupInfo'):
			self._sendMessage(genResNilmGroupInfo(getNilmGroupInfoForClient()))

	def checkNilmPredictOutput(self, message):
		orderSheet = message['orderSheet']
		orderSheet['gids'] = self._checkGroups(orderSheet['groupNames'])
		startTS = orderSheet['startTS']
		endTS = orderSheet['endTS']
		gids = orderSheet['gids']
		sids = self._checkSids(orderSheet)
		outputChecker = NilmOutputChecker(self._logger)
		checkedOutputResultSet = outputChecker.doProcess(startTS, endTS, sids)
		mergedOutputResultSet = self._mergeOutputInfoToGroupName(checkedOutputResultSet, gids)
		try:
			self._sendMessage(genResNilmOutputCheckInfo(mergedOutputResultSet))
		except Exception, e:
			self._closeConnection()


	def _checkJobTypes(self, orderSheet):
		userJobTypes = orderSheet['jobs']
		allowedJobTypes = ['usage', 'meta', 'update', 'candidate', 'status', 'learnschedule']
		if '*' in userJobTypes:
			return allowedJobTypes
		elif len(set(allowedJobTypes).intersection(set(userJobTypes))) == len(userJobTypes):
			return userJobTypes
		else:
			raise ValueError

	def _checkGroups(self, userGroups):
		gids = []
		allowedGroups = getNilmGroupTable()
		allowedGroupNames = allowedGroups.keys()
		
		if '*' in userGroups:
			gids.extend([allowedGroups[groupName] for groupName in allowedGroups.keys()])
		elif len(set(allowedGroupNames).intersection(set(userGroups))) == len(userGroups):
			for groupName in userGroups:
				gids.append(allowedGroups[groupName])
		if gids:
			gids.sort()
		return gids

	def _checkSids(self, orderSheet):
		def checkTargetSids(gids, userSids):
			allGroupSids = self._dataManager.getNilmTargetSids(gids)
			if '*' in userSids:
				return list(set(allGroupSids))
			elif len(set(allGroupSids).intersection(set(userSids))) == len(userSids):
				return list(set(userSids))
			else:
				diffSids = list(set(userSids).difference(set(allGroupSids)))
				self._logger.warn('# wrong user sids : %s' %str(diffSids))
				return None

		gids = self._checkGroups(orderSheet['groupNames'])
		if not gids:
			raise ValueError
			return

		userSids = checkTargetSids(gids, orderSheet['sids'])
		if userSids:
			userSids.sort()

		if not userSids:
			raise ValueError
			return
			# self._logger.warn("# wrong user params : check site ids on group names")
			# self.sendMessage(genReqError("check site ids on group names"))
			# self._sender._socketObj.close()
			# sys.exit(1)
		return list(set(userSids))

	def _getWorkerResoures(self):
		workerResources = copy.deepcopy(self._resourceManager.getWorkerResoures())
		priorityWorkerPool = self._resourceManager.getPriorityWorkerPool()
		privateWorkerPool = self._resourceManager.getPrivateWorkerPool()
		publicWorkerPool = self._resourceManager.getPublicWorkerPool()
		priorityJobPool = self._resourceManager.getPriorityJobPool()
		privateJobPool = self._resourceManager.getPrivateJobPool()
		publicJobPool = self._resourceManager.getPublicJobPool()

		priorityStack = len(self._resourceManager.getPrevWorkerObjs())
		priorityEnabled = self._resourceManager.isTajoEnabled()

		priorityWorkerSize = priorityWorkerPool.qsize()
		privateWorkerSize = privateWorkerPool.qsize()
		publicWorkerSize = publicWorkerPool.qsize()
		priorityJobSize = priorityJobPool.qsize()
		privateJobSize = privateJobPool.qsize()
		publicJobSize = publicJobPool.qsize()

		currentPriorityWorkerId = self._resourceManager.getCurrPriorityWorker()
		if currentPriorityWorkerId:
			if workerResources.has_key(currentPriorityWorkerId):
				workerResources[currentPriorityWorkerId]['priority_runner'] = True
		else:
			for workerId in workerResources.keys():
				workerResources[workerId]['priority_runner'] = False

		workerResourcesMessage = {
			'workers': {
				'priority': priorityWorkerSize,
				'private': privateWorkerSize,
				'public': publicWorkerSize,
				'priority_enabled': str(priorityEnabled)
			},
			'jobs': {
				'priority': priorityJobSize,
				'private': privateJobSize,
				'public': publicJobSize,
				'priority_stack': priorityStack
			},
			'resources': workerResources
		}
		return workerResourcesMessage

	def _mergeOutputInfoToGroupName(self, checkedOutputResultSet, gids):
		mergedOutputResultSet = {}
		groupMap = self._dataManager.getNilmTargetGroupMap(gids)
		for sid in checkedOutputResultSet.keys():
			gname = groupMap[sid]
			if mergedOutputResultSet.has_key(gname):
				mergedOutputResultSet[gname][sid] = checkedOutputResultSet[sid]
			else:
				mergedOutputResultSet[gname] = {sid:checkedOutputResultSet[sid]}
		return mergedOutputResultSet

	def _delNilmInfo(self, orderSheet):
		def doRemove(sids, delTypes):
			self._logger.warn('# Delete NILM data, sids : %s, delTypes : %s' %(str(sids), str(delTypes)))
			for sid in sids:
				if self._dataManager.getRemovableDataService(sid):
					self._dataManager.removeDataService(sid, delTypes)
				else:
					self._logger.warn("- not registrated, sid : %d" %(sid))

		self._logger.warn("########################")
		self._logger.warn("### DELETE Nilm INFO ###")
		self._logger.warn("########################")
		sids = orderSheet['sids']
		delTypes = orderSheet['remove']
		doRemove(sids, delTypes)
		return None

	def _sendMessage(self, message):
		try:
			self._sender.sendMsg(message)
		except Exception, e:
			self._logger.exception(e)

	def _closeConnection(self):
		try:
			self._sender._socketObj.close()
		except Exception, e:
			self._logger.exception(e)
