#!/usr/bin/env python
# -*- coding: utf-8 -*-

from message_pool.system_protocol_messages import genConnectionLost
from socket import socket, AF_INET, SOCK_STREAM
from threading import Thread, Lock
import pickle
import random
import time

BUFFER = 1024

class NetworkHandler:
	def __init__(self, logger, socketObj = None):
		self._logger = logger
		self._socketObj = socketObj
		if socketObj:
			self._isAlive = True
		else:
			self._isAlive = False
		self._autoRecvQ = False
		self._workerId = None
		self._peerName = None
		if socketObj:
			self._peerName = str(socketObj.getpeername())


	def __del__(self):
		self.close()

	def connection(self, hostInfo):
		try:
			socketObj = socket(AF_INET, SOCK_STREAM)
			socketObj.connect(hostInfo)
			self._socketObj = socketObj
			self._isAlive = True
		except Exception, e:
			self._isAlive = False

	def sendMsg(self, protoMessage):
		if protoMessage['protocol'] == 'MW_NET_STAT_HB' or protoMessage['protocol'] == 'WM_NET_STAT_HB':
			pass
		else:
			self._logger.debug(">> send message : %s" %str(protoMessage))
		if not self._socketObj:
			return False
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
			if self._peerName:
				self._logger.error("# socket is closed, addr : %s" %(self._peerName))
			else:
				self._logger.error("# socket is closed")
			self._isAlive = False
			self.close()
			return False
		return True

	def recvMsg(self):
		dataBuffer = []
		try:
			receivedByte = 0
			dataLength = 0
			while True:
				if not self._socketObj:
					return False

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

				if receivedByte >= dataLength:
					break

				data = self._socketObj.recv(min(dataLength - receivedByte, BUFFER))
				dataBuffer.append(data)

				receivedByte = receivedByte + len(str(data))
				if dataLength - receivedByte <= 0:
					break
			protoMessage = pickle.loads("".join(dataBuffer))


		except EOFError, e:
			if self._peerName:
				self._logger.error("# socket is closed, addr : %s" %(self._peerName))
			else:
				self._logger.error("# socket is closed")
			self._isAlive = False
			return None
		except Exception, e:
			if self._peerName:
				self._logger.error("# socket is closed, addr : %s" %(self._peerName))
			else:
				self._logger.error("# socket is closed")
				self._logger.exception(e)
				self._isAlive = False
			return None

		if protoMessage['protocol'] == 'MW_NET_STAT_HB' or protoMessage['protocol'] == 'WM_NET_STAT_HB':
			pass
		else:
			self._logger.debug("<< received data : %s" %str(protoMessage))
		return protoMessage

	def runMsgToQueue(self, queue):
		def _runRecvMsgQ(queue):
			while self.isAlive():
				message = self.recvMsg()
				if message:
					queue.put_nowait(message)
				else:
					self._isAlive = False
					queue.put_nowait(genConnectionLost(self.getWorkerId()))
			if self._socketObj:
				self.close()

		recvQ = Thread(target=_runRecvMsgQ, args=(queue,))
		recvQ.setDaemon(1)
		recvQ.start()

	def isAlive(self):
		return self._isAlive

	def hasWorkerId(self):
		if self._workerId:
			return True
		return False

	def setWorkerId(self, workerId):
		self._workerId = workerId

	def getWorkerId(self):
		return self._workerId

	def close(self):
		try:
			if self._socketObj:
				self._socketObj.close()
				self._logger.info("# client connection closed")
		except Exception, e:
			self._logger.exception(e)
		finally:
			self._isAlive = False