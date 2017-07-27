#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dpu_master.job_container.dpu_output_checker.DpuOutputChecker import DpuOutputChecker
from threading import Thread, Lock

class MasterJobManager:
	def __init__(self, logger):
		self._logger = logger

	def doMasterJob(self):
		if proto == 'REQ_DPU_OUTPUT_CHECK':
			self._logger.debug('- crete job process, job type : %s' %jobType)
			jobThread = Thread(target=self._runOutputChecker, args=(processType, jobId, jobType, message))

		if jobThread:
			jobThread.setDaemon(1)
			jobThread.start()


	def _runOutputChecker(self):
		try:
			outputChecker = DpuOutputChecker(logger=self._outputCheckerLogger)
			params = message['params']
			startTS = params['startTS']
			endTS = params['endTS']
			sids = params['sids']
			result = outputChecker.doProcess(startTS, endTS, sids)
			resultMessage = genResDpuOutputCheck(jobType=jobType, params=result)
		except Exception, e:
			self._logger.exception(e)
