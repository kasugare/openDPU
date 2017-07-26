#!/usr/bin/env python
# -*- coding: utf-8 -*-

from nilm_master.job_container.nilm_output_checker.NilmOutputChecker import NilmOutputChecker
from threading import Thread, Lock

class MasterJobManager:
	def __init__(self, logger):
		self._logger = logger

	def doMasterJob(self):
		if proto == 'REQ_NILM_OUTPUT_CHECK':
			self._logger.debug('- crete job process, job type : %s' %jobType)
			jobThread = Thread(target=self._runOutputChecker, args=(processType, jobId, jobType, message))
				
		if jobThread:
			jobThread.setDaemon(1)
			jobThread.start()

	
	def _runOutputChecker(self):
		try:
			outputChecker = NilmOutputChecker(logger=self._outputCheckerLogger)
			params = message['params']
			startTS = params['startTS']
			endTS = params['endTS']
			sids = params['sids']
			result = outputChecker.doProcess(startTS, endTS, sids)
			resultMessage = genResNilmOutputCheck(jobType=jobType, params=result)
		except Exception, e:
			self._logger.exception(e)
