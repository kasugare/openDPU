#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tcp_modules.NetworkHandler import DataSender

class ProtocolAnalyzer:
	def __init__(self, logger, socketObj=None):
		self._logger = logger
		if socketObj:
			self._sender = DataSender(logger, socketObj)

	def analyzeMessage(self, message):
		if type(message) != dict or not message.has_key('proto'):
			return None
		protocol = message['proto']
		return protocol
