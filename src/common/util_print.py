#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_datetime import convertTS2Date, htime

def printDataPathMap(_logger, rawDataPathMap, jobType = 'app_usage'):
	_logger.debug('# target raw data')
	for sid_did in rawDataPathMap.keys():
		for fid in rawDataPathMap[sid_did].keys():
			dataPathList = rawDataPathMap[sid_did][fid]
			for dataFilePath in dataPathList:
				_logger.debug('- %s_%s : %s' %(sid_did, fid, dataFilePath))

def printMap(logger, statusMap):
	if not statusMap:
		return
	logger.debug("-" * 160)
	for key in statusMap.keys():
		try:
			sid = statusMap[key]['sid']
			dpuFreq = statusMap[key]['dpuFreq']
			deviceCh = statusMap[key]['deviceCh']
			deviceStatus = statusMap[key]['deviceStatus']
			dpuStatus = statusMap[key]['dpuStatus']
			hasMeta = statusMap[key]['hasMeta']
			regTS = statusMap[key]['regTS']
			updateTS = statusMap[key]['updateTS']
			# DATE = convertTS2Date(regTS * 1000)
			logger.debug('sid: %d, freq: %d, ch: %d, devStat: %s, dpuStat: %s, meta: %d, regTS: %d (%s), upTS: %d (%s)' %(sid, dpuFreq, deviceCh, deviceStatus, dpuStatus, hasMeta, regTS, htime(regTS*1000), updateTS, htime(updateTS*1000)))
			# logger.debug('%d - REG_DATE : %d, DATE : %s, DEVICE : %s, DPU : %s, META : %d' %(key, regTS, DATE, deviceStatus, dpuStatus, hasMeta))
		except Exception, e:
			logger.error(statusMap[key])
			logger.exception(e)
	logger.debug("-" * 160)

def printStatusMap(statusMap):
	if not statusMap:
		return

	count =0
	print "-" * 100
	print "< ACTIVATED DEVICE INFO >"
	for key in statusMap.keys():
		deviceStatus = statusMap[key]['deviceStatus']
		dpuStatus = statusMap[key]['dpuStatus']
		hasMeta = statusMap[key]['hasMeta']
		regTS = statusMap[key]['regTS'] * 1000
		DATE = convertTS2Date(regTS)
		if deviceStatus == 'active':
			print '%d - REG_DATE : %d, DATE : %s, DEVICE : %s, DPU : %s, META : %d' %(key, regTS, DATE, deviceStatus, dpuStatus, hasMeta)
		count += 1
	print " count : %d" %(count)
	print "-" * 100

	print "< ALLOWED DPU STATUS INFO >"
	count = 0
	for key in statusMap.keys():
		deviceStatus = statusMap[key]['deviceStatus']
		dpuStatus = statusMap[key]['dpuStatus']
		hasMeta = statusMap[key]['hasMeta']
		regTS = statusMap[key]['regTS'] * 1000
		DATE = convertTS2Date(regTS)
		if dpuStatus == 'active':
			print '%d - REG_DATE : %d, DATE : %s, DEVICE : %s, DPU : %s, META : %d' %(key, regTS, DATE, deviceStatus, dpuStatus, hasMeta)
		count += 1
	print " count : %d" %(count)
	print "-" * 100

	print "< ACTIVATED DEVICE, ALLOWED DPU INFO >"
	count = 0
	for key in statusMap.keys():
		deviceStatus = statusMap[key]['deviceStatus']
		dpuStatus = statusMap[key]['dpuStatus']
		hasMeta = statusMap[key]['hasMeta']
		regTS = statusMap[key]['regTS'] * 1000
		DATE = convertTS2Date(regTS)
		if deviceStatus == 'active' and dpuStatus == 'active':
			print '%d - REG_DATE : %d, DATE : %s, DEVICE : %s, DPU : %s, META : %d' %(key, regTS, DATE, deviceStatus, dpuStatus, hasMeta)
			count += 1
	print " count : %d" %(count)
	print "-" * 100

	print "< ACTIVATED DPU STATUS INFO >"
	count = 0
	for key in statusMap.keys():
		deviceStatus = statusMap[key]['deviceStatus']
		dpuStatus = statusMap[key]['dpuStatus']
		hasMeta = statusMap[key]['hasMeta']
		regTS = statusMap[key]['regTS'] * 1000
		DATE = convertTS2Date(regTS)
		if deviceStatus == 'active' and dpuStatus == 'active' and hasMeta == 1:
			print '%d - REG_DATE : %d, DATE : %s, DEVICE : %s, DPU : %s, META : %d' %(key, regTS, DATE, deviceStatus, dpuStatus, hasMeta)
		count += 1
	print " count : %d" %(count)
	count = 0
