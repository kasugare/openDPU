#!/usr/bin/env python
# -*- coding: utf-8 -*-

from test_app.TestMaster import TestMaster
from test_app.TestWorker import TestWorker

class AppWareHouseManager:
	def __init__(self, logger):
		self._logger = logger

	def getTaskingMaster(self):
		seff._logger.info("# get tasking master object")
		return TestMaster()

	def getTaskingWorker(self):
		seff._logger.info("# get tasking worker object")
		return TestWorker()