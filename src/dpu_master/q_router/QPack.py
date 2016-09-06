#!/usr/bin/env python
# -*- coding: utf-8 -*-

class QPack:
	def __init__(self, logger, clientQ, deployQ, jobQ, resourceQ, networkQ):
		self._logger = logger
		self.clientRouterQ  = clientQ['reqQ']
		self.clientReqQ = clientQ['routerQ']
		self.deployRouterQ  = deployQ['reqQ']
		self.deployReqQ = deployQ['routerQ']
		self.jobRouterQ  = jobQ['reqQ']
		self.jobReqQ = jobQ['routerQ']
		self.resourceRouterQ  = resourceQ['reqQ']
		self.resourceReqQ = resourceQ['routerQ']
		self.networkReqQ = networkQ['reqQ']
		self.networkRouterQ = networkQ['routerQ']

	def __del__(self):
		self._logger.error("QPack")

	def _closeQ(self):
		self._logger.warn('@ close dpu message queues')
		self.clientRouterQ.close()
		self.clientReqQ.close()
		self.deployRouterQ.close()
		self.deployReqQ.close()
		self.jobRouterQ.close()
		self.jobReqQ.close()
		self.resourceRouterQ.close()
		self.resourceReqQ.close()
		self.networkReqQ.close()
		self.networkRouterQ.close()
