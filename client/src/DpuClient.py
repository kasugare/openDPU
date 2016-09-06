#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common_utility import getServerInfo
from UserOrderSheet import UserOptionParser
from MessageGenerator import genOrderMessage, genResourceInfoMessage
from socket import socket, AF_INET, SOCK_STREAM
import pickle

BUFFER = 1024

class DpuClient:
	def __init__(self):
		self._orderSheet = UserOptionParser().getUserOption()
		# print self._orderSheet

	def _connServer(self):
		hostIp, hostPort = getServerInfo()
		socketObj = socket(AF_INET, SOCK_STREAM)
		socketObj.connect((hostIp, hostPort))
		self.sender = DataSender(socketObj)
		self.recevier = DataReceiver(socketObj)

	def doProcess(self):
		self._connServer()
		message = genResourceInfoMessage()
		print message

		while self.sender.sendMsg(message):
			recvMessage = self.recevier.recvMsg()
			if not recvMessage:
				return
			proto = recvMessage['proto']
			if proto == 'REQ_JC':
				print "Job completed"
				break
			elif proto == 'RES_OK':
				print "Job doing..."
				break
			elif proto == 'REQ_ERR':
				print "## Wrong Params : %s ##" %recvMessage['error']
				break
			elif proto == 'RES_ERR':
				print "## Wrong Params : %s ##" %recvMessage['error']
				break

class DataSender:
	def __init__(self, socketObj):
		self._socketObj = socketObj

	def sendMsg(self, protoMessage):
		print ">> send message : %s" %str(protoMessage)
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
			print '[warn] Network error.'
			print traceback.format_exc(e)
			return False
		return True

	def close(self):
		self._socketObj.close()

class DataReceiver:
	def __init__(self, socketObj):
		self._socketObj = socketObj

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

				if receivedByte >= dataLength:
					break

				data = self._socketObj.recv(min(dataLength - receivedByte, BUFFER))
				dataBuffer.append(data)

				receivedByte = receivedByte + len(str(data))
				if dataLength - receivedByte <= 0:
					break

		except Exception, e:
			self._logger.exception(e)

		protoMessage = pickle.loads("".join(dataBuffer))
		print "<< received data : %s" %str(protoMessage)
		return protoMessage

	def close(self):
		self._socketObj.close()

if __name__ == "__main__":
	DpuClient().doProcess()