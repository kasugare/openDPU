#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_common import cvtWorkerId
from threading import Thread, Lock
import pickle
import random
import time

BUFFER = 4096

class DataSender:
	def __init__(self, logger, socketObj):
		self._socketObj = socketObj
		self._workerId = cvtWorkerId(socketObj)
		self._logger = logger
		self._lock = Lock()

	def sendMsg(self, protoMessage):
		if not protoMessage['proto'] == 'RES_HB' and not protoMessage['proto'] == 'REQ_HB':
			self._logger.debug(">> [%s] send : %s" %(self._workerId.ljust(16), str(protoMessage)))
		try:
			protoMessage = pickle.dumps(protoMessage)
			sendMsg = "#%d\r\n%s" %(len(protoMessage), protoMessage)
			msgLength = len(sendMsg)
			totalsent = 0
			while totalsent < msgLength:
				sendLength = self._socketObj.send(sendMsg[totalsent:])
				if sendLength == 0:
					break
				totalsent = totalsent + sendLength
		except Exception, e:
			self._logger.error('# server/client is not available.')
			time.sleep(0.1)
			return False
		return True

	def close(self):
		self._socketObj.close()

class DataReceiver:
	def __init__(self, logger, socketObj, recvMessageQ=None, debugging=None):
		self._socketObj = socketObj
		self._logger = logger
		self._debugging = debugging
		self._recvMessageQ = recvMessageQ
		self._workerId = cvtWorkerId(socketObj)

		if recvMessageQ:
			receiveThread = Thread(target=self._runMsgReceiver, args=(recvMessageQ,))
			receiveThread.setDaemon(1)
			receiveThread.start()


	def recvMsg(self):
		dataBuffer = []
		try:
			receivedByte = 0
			dataLength = 0
			while True:
				if receivedByte == 0:
					data = self._socketObj.recv(BUFFER)
					lengthIndex = data.find('\r\n')

					if lengthIndex == -1:
						return
					if not data:
						break

					dataLength = int(data[:lengthIndex].split('#')[1])
					rawData = data[lengthIndex+2:]
					dataBuffer.append(rawData)
					receivedByte = len(rawData)

				if receivedByte == dataLength:
					break
				elif receivedByte > dataLength:
					break

				data = self._socketObj.recv(min(dataLength - receivedByte, BUFFER))
				dataBuffer.append(data)

				receivedByte = receivedByte + len(str(data))
				if dataLength - receivedByte <= 0:
					break

		except Exception, e:
			self._logger.exception(e)

		protoMessage = pickle.loads("".join(dataBuffer))

		if not protoMessage['proto'] == 'RES_HB' and not protoMessage['proto'] == 'REQ_HB':
			self._logger.debug("<< [%s] receive : %s" %(self._workerId.ljust(16), str(protoMessage)))
		return protoMessage

	def recvMsgThroughQ(self, recvMessageQ):
		self._runMsgReceiver(recvMessageQ)

	def _runMsgReceiver(self, recvMessageQ=None):
		def putMessageQ(message):
			if type(message) == dict:
				message['networkId'] = cvtWorkerId(self._socketObj)
			recvMessageQ.put_nowait(message)

		if not recvMessageQ:
			self._logger.warn("It can not use Q receiver. You have to set Queue when make this instance")
			return None

		try:
			dataBuffer = []
			receivedByte = 0
			dataLength = 0
			while True:
				if receivedByte == 0:
					data = self._socketObj.recv(BUFFER)
					lengthIndex = data.find('\r\n')
					if lengthIndex == -1 or not data or len(data) == 0:
						break

					dataLength = int(data[:lengthIndex].split('#')[1])
					rawData = data[lengthIndex+2:]
					dataBuffer.append(rawData)
					receivedByte = len(rawData)
				else:
					data = self._socketObj.recv(min(dataLength - receivedByte, BUFFER))
					if not data or len(data) == 0:
						break
					dataBuffer.append(data)
					receivedByte += len(data)

				if receivedByte == dataLength:
					protoMessage = pickle.loads("".join(dataBuffer))
					putMessageQ(protoMessage)
					# recvMessageQ.put_nowait(protoMessage)
					receivedByte = 0
					dataLength = 0
					dataBuffer = []
				elif receivedByte < dataLength:
					pass

				while receivedByte > dataLength:
					bufferedData = "".join(dataBuffer)
					messageData = bufferedData[:dataLength]
					protoMessage = pickle.loads(messageData)
					putMessageQ(protoMessage)
					# recvMessageQ.put_nowait(protoMessage)
					dataBuffer = []

					bufferedData = bufferedData[dataLength:]
					lengthIndex = bufferedData.find('\r\n')
					if lengthIndex:
						if len(bufferedData[:lengthIndex].split('#')) >= 2:
							dataLength = int(bufferedData[:lengthIndex].split('#')[1])
							bufferedData = bufferedData[lengthIndex+2:]
							receivedByte = len(bufferedData)
							dataBuffer.append(bufferedData)

						if receivedByte == dataLength:
							protoMessage = pickle.loads("".join(dataBuffer))
							putMessageQ(protoMessage)
							# recvMessageQ.put_nowait(protoMessage)
							receivedByte = 0
							dataLength = 0
							dataBuffer = []
					else:
						rawData = self._socketObj.recv(BUFFER)
						dataBuffer.append(bufferedData)
						dataBuffer.append(rawData)
						bufferedData = "".join(dataBuffer)
						lengthIndex = bufferedData.find('\r\n')
						if lengthIndex == -1:
							break
						dataLength = int(bufferedData[:lengthIndex].split('#')[1])
						rawData = data[lengthIndex+2:]
						receivedByte += len(rawData)
						dataBuffer = []
						dataBuffer.append(rawData)

				if type(protoMessage) == dict and not protoMessage['proto'] == 'RES_HB' and not protoMessage['proto'] == 'REQ_HB':
					self._logger.debug("<< [%s] receive : %s" %(self._workerId.ljust(16), str(protoMessage)))
		except Exception, e:
			self._logger.error("# worker is not available")
			self._logger.exception(e)
		finally:
			putMessageQ("RESET")
			# recvMessageQ.put_nowait("RESET")

	def getMessageQueue(self):
		if not self._recvMessageQ:
			self._logger.error("It can not use Q receiver. You have to set Queue when make this instance")
			raise AssertionError
		return self._recvMessageQ

	def close(self):
		self._socketObj.close()
