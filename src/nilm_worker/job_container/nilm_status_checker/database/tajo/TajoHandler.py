#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_datetime import convertTS2Date
from TajoGatewayHelper import TajoGatewayHelper
from TajoQueryPool import TajoQueryPool

class TajoHandler(TajoGatewayHelper):
	def __init__(self, logger):
		TajoGatewayHelper.__init__(self, logger)
		self._logger = logger
		self._queryPool = TajoQueryPool(logger)

	def _executeQuery(self, queryString):
		# self._logger.debug(' - tajo query string : %s' %queryString)
		self.sendProtocol(queryString)
		rawDataList = self.getRawData()
		return rawDataList

	def selectActiveDeviceSiteMap(self, startTS, endTS):
		queryList = self._queryPool.getSelectActiveDeviceSiteMap(startTS, endTS)
		deviceSiteMap = {}

		for queryString in queryList:
			items = self._executeQuery(queryString)
			for item in items:
				try:
					splitedItem = item.replace('  ','').split(',')
					dts = int(splitedItem[0])
					sid = int(splitedItem[1])
					did = int(splitedItem[2])
					deviceCh = int(splitedItem[3])
					deviceFreq = int(splitedItem[4])
					nilmFreq = deviceFreq
					if deviceCh > 1:
						deviceCh -= 1
					if deviceCh == 2:
						deviceFreq = 10
						nilmFreq = 10
					deviceSiteMap[sid] = {
						'dts': dts,
						'did': did,
						'deviceCh': deviceCh,
						'deviceFreq': deviceFreq,
						'nilmFreq': nilmFreq
					}
				except Exception, e:
					self._logger.exception(e)
					self._logger.error(item)
		return deviceSiteMap
