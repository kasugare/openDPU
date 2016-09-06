#!/usr/bin/env python
# -*- coding: utf-8 -*-

from util_datetime import convertOneDayBaseTimestampGmt9
import traceback
import os

def printDataPathMap(_logger, rawDataPathMap, jobType = 'app_usage'):
	_logger.debug('# target raw data')
	for sid_did in rawDataPathMap.keys():
		for fid in rawDataPathMap[sid_did].keys():
			dataPathList = rawDataPathMap[sid_did][fid]
			for dataFilePath in dataPathList:
				_logger.debug('- %s_%s : %s' %(sid_did, fid, dataFilePath))