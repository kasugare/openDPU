#!/usr/bin/env python
# -*- coding: utf-8 -*-

from data_store.DataServiceManager import DataServiceManager
import os

class DeviceMetaHandler:
	def __init__(self, _logger):
		self._logger = _logger
		self._orderSheet = None
		self.deviceMetaPath = self._getMetaDirPath()

	def _getMetaDirPath(self):
		dirPath = os.path.abspath('../meta')
		try:
			if not os.path.exists(dirPath):
				os.makedirs(dirPath)
		except Exception, e:
			self._logger.exception(e)
		return dirPath

	def setOrderSheet(self, orderSheet):
		self._orderSheet = orderSheet
