#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.conf_version import getVersionInfo
from common.conf_nilm_groups import getNilmGroupTable, getNilmGroupInfoForClient
from common.util_common import cvtWorkerId as cvtClientId
from data_store.DataServiceManager import DataServiceManager
import copy
import sys

class ClientJobResultHandler:
	def __init__(self, logger, socketObj, resourceManager):
		self._logger = logger
		self._socketObj = socketObj
		self._resourceManager = resourceManager
		self._clintId = cvtClientId(socketObj)

	def doNilmMlResultJob(self, message):
		self._sendMessage(genResOK())
		self._resourceManager.delClientObject(clientId)
