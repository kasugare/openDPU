#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_collector import getDataCollector, getBulkSize
from common.conf_network import getHostInfo
from protocol.message_pool.MessageGenerator import genResHB, genReqConnClose, genResOK
from dpu_master.collection_manager.CollectionMessageGenerator import CollectionMessageGenerator
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
			self._logger.debug("# Job task assign")

			try:
				proto = message['proto']
				if proto == 'REQ_DPU_JOB':
					jobId, reqMsgs = self._messageGenerator.genCollectionMessages(message['params'])
					self._genDpuRawDataRequestSet(jobId, reqMsgs)

			except Exception, e:
				self._logger.exception(e)
				self._logger.error("# Data collector server not ready.")

	def _genDpuRawDataRequestSet(self, jobId, reqMsgs):
		primeryJobTypes = ['statusCheck']
		secondaryJobTypes = ['usage', 'ETL']
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
			jobType = orderedJob['jobType']
			jobId = orderedJob['jobId']
			taskId = orderedJob['taskId']

			self._resourceHandler.addIdleJobDAG(jobId, taskId)

			if jobType in primeryJobTypes:
				primeryJobs.append(orderedJob)

			elif jobType in secondaryJobTypes:
				secondaryJobs.append(orderedJob)

			elif jobType in tertiaryJobTypes:
				tertiaryJobs.append(orderedJob)

			elif jobType in quaternaryJobTypes:
				quaternaryJobs.append(orderedJob)

			elif jobType in quinaryJobTypes:
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
		if secondaryJobs: orderJobs(secondaryJobs, priority=2, processType='privateCpu')
		if tertiaryJobs: orderJobs(tertiaryJobs, priority=3, processType='priorityCpu')
		if quaternaryJobs: orderJobs(quaternaryJobs, priority=1, processType='publicCpu')
		if quinaryJobs: orderJobs(quinaryJobs, priority=4, processType='priorityCpu')
