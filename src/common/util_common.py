#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
	