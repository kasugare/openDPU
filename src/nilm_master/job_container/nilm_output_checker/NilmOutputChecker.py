#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from common.util_datetime import htime, getUtcNowTS, getDailyDatetime
from NilmOutputManager import NilmOutputManager

PROCESS_NAME = 'NILM_OUTOUT_CHECKER'

class NilmOutputChecker:
	def __init__(self, logger=None, loggerMode='DEBUG'):
		if logger:
			self._logger = logger
		else:
			self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.setLevel(loggerMode)
		self._nilmStatusMap = {}

	def doProcess(self, startTS, endTS, sids=[]):
		self._logger.info("######################################")
		self._logger.info("### DO Process Nilm Output Checker ###")
		self._logger.info("######################################")

		outputManager = NilmOutputManager(self._logger)
		checkedOutputResultSet = outputManager.doProcess(startTS, endTS, sids)
		return checkedOutputResultSet

if __name__=='__main__':
	sids = [10006298, 10006307, 10006311, 10006315, 10006325, 10017937, 10011723, 10011827, 10011968, 10011971, 10011983, 10011987, 10012000, 10012011, 10012012, 10012078, 10012082, 10012085, 10012086, 10012125, 10012132, 10012167, 10012168, 10012171, 10012172, 10012179, 10012180, 10012188, 10012189, 10012192, 10012197, 10012203, 10012204, 10012209, 10012226, 10012227, 10012239, 10012241, 10012243, 10012250, 10012251, 10012252, 10012256, 10012257, 10012259, 10012266, 10012271, 10012272, 10012293, 10012296, 10012297, 10012298, 10012310, 10012311, 10012314, 10012316, 10012317, 10012318, 10012322, 10012324, 10012335, 10012336, 10012340, 10012341, 10012347, 10012350, 10012356, 10012358, 10012366, 10012390, 10012392, 10012412, 10012413, 10012446, 10012476, 10012480, 10012488, 10012489, 10012542, 10012545, 10012557, 10012558, 10012885, 10013109, 10017468, 10017478, 10017479, 10017480, 10017481, 10017483, 10017484, 10017485, 10017486, 10017806, 10017807, 10022327, 10022334]
	
	currentTS = getUtcNowTS() * 1000
	startTS, endTS = getDailyDatetime(currentTS)
	startTS = 1471825078000
	
	outputChecker = NilmOutputChecker()
	outputChecker.doProcess(startTS, endTS, sids)
