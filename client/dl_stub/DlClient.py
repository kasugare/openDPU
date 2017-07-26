#!/usr/bin/env python
# -*- coding: utf-8 -*-

from optparse import OptionParser
from threading import Thread, Lock
import socket
import traceback
import pickle
import time
import sys
import os

PROCESS_NAME = "STUB_WORKER"
BUFFER = 8192
HEART_BEAT_TIME = 60

class DlClient:
	def __init__(self, hostIp=None, hostPort=16100):
		if not hostIp:
			hostIp = socket.gethostbyname(socket.gethostname())
		self._hostIp = hostIp
		self._hostPort = hostPort
		self._orderSheet = UserOptions().getUserOption()

	def doProcess(self, message=None):
		message = {
			"proto": "REQ_STUB_NILM_ML",
			"jobType": "NILM_ML",
			"params": self._orderSheet
			}

		socketObj = self._connStubServer(self._hostIp, self._hostPort)
		self._checkHB(socketObj)
		DataSender(False, socketObj).sendMsg(message)
		mlResultMessage = DataReceiver(True, socketObj).recvMsg()

		if not mlResultMessage or type(mlResultMessage) != dict or not mlResultMessage.has_key('proto'):
			return 
		protocol = mlResultMessage['proto']
		if protocol == 'RES_STUB_NILM_ML_SUCCESS':
			resultMessage = mlResultMessage['result']
			sys.stdout.write(resultMessage+"\n")


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

class UserOptions:
	def __init__(self):
		self._options()

	def _options(self):
		usage = """usage: %prog [options] arg1,arg2 [options] arg1
$ python NilmCluster.py -v -p 2015-01-01_00:00:00 2015-01-02_23:59:59 -s 1 -D edm3_090"""

		parser = OptionParser(usage = usage)
		parser.add_option("-s", "--sid",
			action="store",
			dest="sid",
			type="int",
			default=None,
			help="""nilm target site id""")

		parser.add_option("-i", "--input",
			action="store",
			dest="trainingDataFilePath",
			type="str",
			default=None,
			help="""input for model training""")

		parser.add_option("-o", "--output",
			action="store",
			dest="savedOutputDirPath",
			type="str",
			default=None,
			help="""directory for output""")

		parser.add_option("-m", "--model",
			action="store",
			dest="modelName",
			type="str",
			default=None,
			help="""Model to test""")

		parser.add_option("-w", "--weights",
			action="store",
			dest="weightFileName",
			type="str",
			default=None,
			help="""model weights""")

		parser.add_option("-p", "--points",
			action="store",
			dest="numOfPoints",
			type="int",
			default=0,
			help="""number of points for sampling""")

		parser.add_option("-f", "--format",
			action="store",
			dest="dataFormat",
			type="str",
			default=None,
			help="""data format for processing""")

		parser.add_option("-c", "--country",
			action="store",
			dest="countryCode",
			type="str",
			default="KR",
			help="""country info""")

		parser.add_option("-z", "--timezone",
			action="store",
			dest="timezone",
			type="str",
			default="Asia/Seoul",
			help="""timezone info""")

		parser.add_option("-t", "--threshold",
			action="store",
			dest="threshold",
			type="float",
			default=0.0,
			help="""threshold""")

		parser.add_option("-x", "--format_option",
			action="store",
			dest="formatOpt",
			type="str",
			default=None)

		options, args = parser.parse_args()
		self._vaildOptions(options, args)

	def _vaildOptions(self, options, args):
		optVaildator = OptionValidator(options, args)
		self.userOptions = optVaildator.checkUserOptions()

	def getUserOption(self):
		return self.userOptions

class OptionValidator:
	def __init__(self, options, args):
		self._userOptions = self._setUserOptions(options, args)

	def _setUserOptions(self, options, args):
		user_options = {}
		if options.sid != None:
			user_options['sid'] = options.sid
		if options.trainingDataFilePath != None:
			user_options['trainingDataFilePath'] = options.trainingDataFilePath
		if options.savedOutputDirPath != None:
			user_options['savedOutputDirPath'] = options.savedOutputDirPath
		if options.modelName != None:
			user_options['modelName'] = options.modelName
		if options.weightFileName != None:
			user_options['weightFileName'] = options.weightFileName
		if options.numOfPoints != None:
			user_options['numOfPoints'] = options.numOfPoints
		if options.dataFormat != None:
			user_options['dataFormat'] = options.dataFormat
		if options.countryCode != None:
			user_options['countryCode'] = options.countryCode
		if options.timezone != None:
			user_options['timezone'] = options.timezone
		if options.threshold != None:
			user_options['threshold'] = options.threshold
		if options.formatOpt != None:
			user_options['formatOpt'] = options.formatOpt
		return user_options

	def checkUserOptions(self):
		userOptions = {}
		userOptions['sid'] = self._checkSiteId(self._userOptions.get('sid'))
		userOptions['trainingDataFilePath'] = self._checkFilePath(self._userOptions.get('trainingDataFilePath'))
		userOptions['savedOutputDirPath'] = self._checkFilePath(self._userOptions.get('savedOutputDirPath'))
		userOptions['modelName'] = self._checkNone(self._userOptions.get('modelName'))
		userOptions['weightFileName'] = self._checkNone(self._userOptions.get('weightFileName'))
		userOptions['numOfPoints'] = self._userOptions.get('numOfPoints')
		userOptions['dataFormat'] = self._checkNone(self._userOptions.get('dataFormat'))
		userOptions['countryCode'] = self._userOptions.get('countryCode')
		userOptions['timezone'] = self._userOptions.get('timezone')
		userOptions['threshold'] = self._userOptions.get('threshold')
		userOptions['formatOpt'] = self._checkExt(self._userOptions.get('formatOpt'))
		return userOptions

	def _checkSiteId(self, sid):
		sid = self._userOptions.get('sid')
		if not sid:
			print("# Wrong site id option")
			sys.exit(1)
		return sid

	def _checkFilePath(self, filePath):
		if not filePath or not os.path.exists(filePath):
			print("# No such file or directory %s" %(filePath))
			sys.exit(1) 
		return filePath

	def _checkNone(self, userOption):
		if not userOption:
			print("# Wrong Options")
			sys.exit(1) 
		return userOption

	def _checkInt(self, userOption):
		if type(userOption) == int:
			return userOption
		elif type(userOption) == str and userOption.isdigit():
			return int(userOption)
		else:
			print("# Wrong Options")
			sys.exit(1) 

	def _checkExt(self, formatOpt):
		if not formatOpt:
			sys.exit(1)
		return formatOpt

class DataSender:
	def __init__(self, debugMode, socketObj):
		self._socketObj = socketObj
		self._debugMode = debugMode

	def sendMsg(self, protoMessage):
		# if self._debugMode:
		# 	print " >> send message : %s" %str(protoMessage)
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
		# if self._debugMode:
			# print " << received data : %s" %str(protoMessage)
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
			"numOfPoints": 10,
			"formatOpt": "TEST"
			}
		}

	client = DlClient(hostIp='127.0.0.1')
	client.doProcess(message)
