#!/usr/bin/env python
# -*- coding: utf-8 -*-

from socket import socket, AF_INET, SOCK_STREAM
from common.conf_collector import getTajoGateway
import traceback
import pickle
import time
import sys
import os

class TajoGatewayHelper:
	def __init__(self, logger):
		self._logger = logger
		self.socketObj = None

	def sendProtocol(self, queryString):
		self._getConnection()
		protocol = self._genProtocol(queryString)
		self._sendMessage(protocol)

	def _getConnection(self):
		if self.socketObj:
			self.socketObj.close()
		tajoConf = getTajoGateway()
		self.socketBuffer = int(tajoConf['buffer'])
		self.socketObj = socket(AF_INET, SOCK_STREAM)
		self.socketObj.connect((tajoConf['host'], int(tajoConf['port'])))

	def _genProtocol(self, queryString):
		message = {'protocol': 'recovery', 'query': "%s" %queryString}
		return message

	def _sendMessage(self, message):
		try:
			encodedMsg = pickle.dumps(message)
			sendMsg = "#%d\r\n%s" %(len(encodedMsg), encodedMsg)
			msgLength = len(sendMsg)
			totalsent = 0

			while totalsent < msgLength:
				sendLength = self.socketObj.send(sendMsg[totalsent:])
				if sendLength == 0:
					break
				totalsent = totalsent + sendLength

		except Exception, e:
			self._logger.error(traceback.format_exc(e))

	def getRawData(self):
		dataBuffer = []
		try:
			receivedByte = 0
			msgLength = 0
			while True:
				if receivedByte == 0:
					data = self.socketObj.recv(self.socketBuffer)
					lengthIndex = data.find('\r\n')

					if lengthIndex == -1:
						return
					if not data:
						break

					msgLength = int(data[:lengthIndex].split('#')[1])
					rawData = data[lengthIndex+2:]
					dataBuffer.append(rawData)
					receivedByte = len(rawData)

				if receivedByte >= msgLength:
					break
				data = self.socketObj.recv(min(msgLength - receivedByte, self.socketBuffer))
				dataBuffer.append(data)

				receivedByte = receivedByte + len(str(data))
				if msgLength - receivedByte <= 0:
					break
		except Exception, e:
			self._logger.error(traceback.format_exc(e))

		csvRawData = pickle.loads("".join(dataBuffer))
		rawDataList = []
		for rawData in csvRawData.split('\n'):
			if rawData:
				rawDataList.append(rawData)
		return rawDataList

	def _showProgress(self):
		while True:
			processRate = self.getRawData()
			header = processRate[0][:1]
			if header == 'P':
				self._logger.info(" - %s" %processRate[0])
			elif header == '-' or header =='(':
				self._logger.info("TajoGateway._showProgress():: transmitting of the raw data ... ")
				break
