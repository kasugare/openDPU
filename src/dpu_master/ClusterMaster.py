#!/usr/bin/env python
# -*- coding: utf-8 -*-

class ClusterMaster:
	def __init__(self):
		self.processObjs = {}

	def setProcessMapQueue(self, pid, processObj):
		self.processObjs[pid] = processObj

	def getProcessMapQueue(self, pid = None):
		if pid and self.processObjs.has_key(pid):
			processObj = self.processObjs[pid]
			return processObj
		return self.processObjs
