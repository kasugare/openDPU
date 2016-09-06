#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from data_store.DataServiceManager import DataServiceManager
import time

PROCESS_NAME = 'OUTPUT_MANAGER'

class OutputManager:
	def __init__(self, outputQ):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('---------- [ Output Manager ] ----------')
		self._dataManager = DataServiceManager(self._logger)
		self._outputQ = outputQ

	def run(self):
		while True:
			self._logger.info("# Ready to run for nilm usage insert #")
			outputMsg = self._outputQ.get()
			try:
				if outputMsg['jobType'] == 'USAGE' or outputMsg['jobType'] == 'usage':

					dailyBasedTS = outputMsg['dbTS']
					sid = outputMsg['sid']
					did = outputMsg['did']
					lfid = outputMsg['lfid']
					hourlyUsage = outputMsg['hourlyUsage']
					dailyUsage = outputMsg['dailyUsage']
					hourlyAppOn = outputMsg['hourlyAppOn']
					dailyAppOn = outputMsg['dailyAppOn']

					self._logger.info(">"*50)
					self._logger.info('# Q size : %d, sid : %d, did : %d, lfid : %d' %(self._outputQ.qsize(), sid, did, lfid))
					self._logger.info("<"*50)
					# self._logger.debug(' - hourlyUsage : %s' %str(hourlyUsage))
					# self._logger.debug(' - dailyUsage  : %s' %str(dailyUsage))
					# self._logger.debug(' - hourlyAppOn : %s' %str(hourlyAppOn))
					# self._logger.debug(' - dailyAppOn  : %s' %str(dailyAppOn))

					self._dataManager.setNilmHourlyFeederUsage(dailyBasedTS, sid, did, lfid, hourlyUsage, hourlyAppOn)
					self._dataManager.setNilmDailyFeederUsage(dailyBasedTS, sid, did, lfid, dailyUsage, dailyAppOn)
					time.sleep(0.2)
			except Exception, e:
				self._logger.error("# output message : %s" %str(outputMsg))
				self._logger.exception(e)
				self._outputQ.put_nowait(outputMsg)
				time.sleep(1)