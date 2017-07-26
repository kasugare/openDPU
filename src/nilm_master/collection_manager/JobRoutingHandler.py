#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_collector import getDataCollector, getBulkSize
from common.conf_network import getHostInfo
from protocol.message_pool.MessageGenerator import genResHB, genReqConnClose, genResOK
from nilm_master.collection_manager.CollectionMessageGenerator import CollectionMessageGenerator
import pickle
import copy
import time

class JobRoutingHandler:
	def __init__(self, logger, resourceHandler, clientRequestJobQueue):
		self._resourceHandler = resourceHandler
		self._messageGenerator = CollectionMessageGenerator(logger)
		self._clientRequestJobQueue = clientRequestJobQueue
		self._logger = logger

	def doProcess(self):
		while True:
			message = pickle.loads(self._clientRequestJobQueue.get())
			self._logger.debug("# Nilm tajo gateway jobs")

			try:
				proto = message['proto']
				if proto == 'REQ_NILM_RAW_DATA':
					reqMsg = self._messageGenerator.genCollectionMessages(message['params'])
					self._genNilmRawDataRequestSet(reqMsg)

			except Exception, e:
				self._logger.exception(e)
				self._logger.error("# Data collector server not ready.")

	def _genNilmRawDataRequestSet(self, reqMsgs):
		primeryJobTypes = ['statusCheck']
		secondaryJobTypes = ['usage']
		tertiaryJobTypes = ['candidate']
		quaternaryJobTypes = ['learnSchedule']
		quinaryJobTypes = ['metaSearch', 'metaUpdate']

		primeryJobs = []
		secondaryJobs = []
		quaternaryJobs = []
		tertiaryJobs = []
		quinaryJobs = []

		for orderedJob in reqMsgs:
			params = orderedJob['params']
			analysisType = params['analysisType']

			if analysisType in primeryJobTypes:
				primeryJobs.append(orderedJob)
			elif analysisType in secondaryJobTypes:
				siteMap = params['target']
				sids = siteMap.keys()
				splitSize = getBulkSize()
				indexCount = 0
				startIndex = 0
				endIndex = splitSize
				splitedSids = []

				while True:
					indexCount += len(sids[startIndex:endIndex])
					splitedSids.append(sids[startIndex:endIndex])
					startIndex = endIndex
					endIndex += splitSize
					if len(sids) <= indexCount: break

				usageParamMap = {}
				for targetSids in splitedSids:
					targetSids = list(set(targetSids))
					usageParamMap = copy.deepcopy(orderedJob)
					usageParamMap['params']['target'] = {}
					for sid in targetSids:
						did = siteMap[sid]
						usageParamMap['params']['target'][sid] = did
					if usageParamMap['params']['target']:
						secondaryJobs.append(usageParamMap)
			elif analysisType in tertiaryJobTypes:
				siteMap = params['target']
				sids = siteMap.keys()
				splitSize = getBulkSize()
				indexCount = 0
				startIndex = 0
				endIndex = splitSize
				splitedSids = []

				while True:
					indexCount += len(sids[startIndex:endIndex])
					splitedSids.append(sids[startIndex:endIndex])
					startIndex = endIndex
					endIndex += splitSize
					if len(sids) <= indexCount: break

				usageParamMap = {}
				for targetSids in splitedSids:
					targetSids = list(set(targetSids))
					usageParamMap = copy.deepcopy(orderedJob)
					usageParamMap['params']['target'] = {}
					for sid in targetSids:
						did = siteMap[sid]
						usageParamMap['params']['target'][sid] = did
					if usageParamMap['params']['target']:
						tertiaryJobs.append(usageParamMap)
			elif analysisType in quaternaryJobTypes:
				quaternaryJobs.append(orderedJob)
			elif analysisType in quinaryJobTypes:
				quinaryJobs.append(orderedJob)

		def orderJobs(jobList, priority=1, processType='publicCpu'):
			if processType == 'priorityCpu':
				jobPool = self._resourceHandler.getPriorityJobPool()
			# elif processType == 'privateCpu':
			# 	jobPool = self._resourceHandler.getPrivateJobPool()
			# else:
				# jobPool = self._resourceHandler.getPublicJobPool()
			else:
				jobPool = self._resourceHandler.getPrivateJobPool()

			for orderedJob in jobList:
				jobPool.put_nowait((priority, orderedJob))

		if primeryJobs: orderJobs(primeryJobs, priority=1, processType='priorityCpu')
		if secondaryJobs: orderJobs(secondaryJobs, priority=2, processType='priorityCpu')
		if tertiaryJobs: orderJobs(tertiaryJobs, priority=3, processType='priorityCpu')
		if quaternaryJobs: orderJobs(quaternaryJobs, priority=1, processType='publicCpu')
		if quinaryJobs: orderJobs(quinaryJobs, priority=4, processType='priorityCpu')
