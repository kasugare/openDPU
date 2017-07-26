#!/usr/bin/env python
# -*- coding: utf-8 -*-

from threading import Thread, Lock
import socket
import traceback
import pickle
import time

PROCESS_NAME = "STUB_WORKER"
BUFFER = 8192
HEART_BEAT_TIME = 1

class StubClient:
	def __init__(self, hostIp=None, hostPort=16100):
		if not hostIp:
			hostIp = socket.gethostbyname(socket.gethostname())
		self._hostIp = hostIp
		self._hostPort = hostPort

	def doProcess(self, message):
		socketObj = self._connStubServer(self._hostIp, self._hostPort)
		self._checkHB(socketObj)
		DataSender(True, socketObj).sendMsg(message)
		DataReceiver(True, socketObj).recvMsg()


	def _connStubServer(self, hostIp, hostPort):
		socketObj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		socketObj.connect((hostIp, hostPort))
		return socketObj

	def _checkHB(self, socketObj):
		heartBeatThread = Thread(target=self._runHeartBeat, args=(socketObj,))
		heartBeatThread.setDaemon(1)
		heartBeatThread.start()

	def _runHeartBeat(self, socketObj):
		try:
			message = {'proto': 'REQ_HB'}
			while True:
				time.sleep(HEART_BEAT_TIME)
				DataSender(True, socketObj).sendMsg(message)
		except Exception, e:
			print traceback.format_exc(e)


		
class DataSender:
	def __init__(self, debugMode, socketObj):
		self._socketObj = socketObj
		self._debugMode = debugMode

	def sendMsg(self, protoMessage):
		if self._debugMode:
			print " >> send message : %s" %str(protoMessage)
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
	def __init__(self, debugMode, socketObj):
		self._socketObj = socketObj
		self._debugMode = debugMode

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
			print traceback.format_exc(e)

		protoMessage = pickle.loads("".join(dataBuffer))
		if self._debugMode:
			print " << received data : %s" %str(protoMessage)
		return protoMessage

	def close(self):
		self._socketObj.close()

if __name__ == '__main__':
	message = {
		"proto": "REQ_STUB_NILM_ML",
		"jobType": "NILM_ML",
		"params": {
			"sid": "10010592",
			"mlDataFilePath": "/Users/kasugare/Work/Encored/nilm_cluster/data/machine_learning_raw_data.csv",
			"modelFileName": "tv_2ch_cnn_lstm_12.h5",
			"country": "KR",
			"timezone": "Asia/Seoul",
			"modelName": "MODEL_NAME",
			"dataFormat": "DATA_FORMAT",
			"threshold": 0.231,
			"numOfPoint": 10
			}
		}

	client = StubClient(hostIp='127.0.0.1')
	client.doProcess(message)
