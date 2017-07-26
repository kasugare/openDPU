#!/usr/bin/env python
# -*- coding: utf-8 -*-

import locale
import sys
import os

def getKeyElements(sid_did):
	splitedKey = sid_did.split('_')
	sid = int(splitedKey[0])
	did = int(splitedKey[1])
	return sid, did

def getNumOfFilesLine(logfiles):
	number_of_line = 0
	for logfile in logfiles:
		number_of_line += len(open(logfile).readlines())
	return number_of_line

def convertDictToString(dictionary):
	if not isinstance(dictionary, dict):
		if isinstance(dictionary, unicode):
			return str(dictionary)
		else:
			return dictionary
	return dict((str(key), convertDictToString(value)) for key, value in dictionary.items())

def cvtWorkerId(workerObj):
	try:
		workerId = '%s_%d' %(workerObj.getpeername()[0], workerObj.getpeername()[1])
	except:
		return None
	return workerId

def cvtNumFormat(number):
	locale.setlocale(locale.LC_ALL, '')
	if str(number).find('.') != -1:
		cvtNumber = locale.format('%.3f', number, 1)
	else:
		cvtNumber = locale.format('%d', number, 1)
	return cvtNumber
