#!/usr/bin/env python
# -*- coding: utf-8 -*-

class Resources:
	def __init__(self, logger):
		self._logger = logger
		self._resources = {}

	def initResource(self, workerID, workerResource):
		self._resources[workerID] = workerResource

	def assignJobProcess(self, processName):
		for workerID in self._resources.keys():
			if processName in self._resources[workerID].keys():
				processInfos = self._resources[workerID][processName]
				if processInfos['idle'] > 0:
					processInfos['idle'] -= 1
					processInfos['working'] += 1
					return

	def dropJobProcess(self, processName):
		for workerID in self._resources.keys():
			if processName in self._resources[workerID].keys():
				processInfos = self._resources[workerID][processName]
				processInfos['idle'] += 1
				processInfos['working'] -= 1

	def setJobProcess(self, workerID, processName, processInfos):
		if self._resources.has_key(workerID):
			self._resources[workerID][processName] = processInfos

	def delResource(self, workerID):
		if self._resources.has_key(workerID):
			del self._resources[workerID]

	def clear(self):
		self.resources.clear()