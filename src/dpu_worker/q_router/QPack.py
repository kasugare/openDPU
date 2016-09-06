#!/usr/bin/env python
# -*- coding: utf-8 -*-

class QPack:
	def __init__(self, logger, deployQ, jobQ, resourceQ, networkQ):
		self._logger = logger
		self.deployReqQ  = deployQ['reqQ']
		self.deployRouterQ = deployQ['routerQ']
		self.jobReqQ  = jobQ['reqQ']
		self.jobRouterQ = jobQ['routerQ']
		self.resourceReqQ  = resourceQ['reqQ']
		self.resourceRouterQ = resourceQ['routerQ']
		self.networkReqQ = networkQ['reqQ']
		self.networkRouterQ = networkQ['routerQ']

	def __del__(self):
		self._logger.error("QPack")

	def _closeQ(self):
		self._logger.warn('@ close dpu message queues')
		self.deployReqQ.close()
		self.deployRouterQ.close()
		self.jobReqQ.close()
		self.jobRouterQ.close()
		self.resourceReqQ.close()
		self.resourceRouterQ.close()
		self.networkReqQ.close()
		self.networkRouterQ.close()