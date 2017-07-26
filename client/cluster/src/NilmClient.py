#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common_utility import getServerInfo, getMailInfo, hdate
from UserOrderSheet import UserOptionParser
from MessageGenerator import genOrderMessage, genVersionCheck, genNilmGroupInfo, genReqWorkersResources, genReqOutputCheck
from socket import socket, AF_INET, SOCK_STREAM
from Logger import Logger
import traceback
import pickle

BUFFER = 1024

class NilmClient:
	def __init__(self):
		self._orderSheet = UserOptionParser().getUserOption()
		self._logger = Logger().getLogger()

	def _connServer(self):
		hostIp, hostPort = getServerInfo()
		socketObj = socket(AF_INET, SOCK_STREAM)
		socketObj.connect((hostIp, hostPort))
		debugMode = self._orderSheet['debugMode']
		self.sender = DataSender(debugMode, socketObj)
		self.recevier = DataReceiver(debugMode, socketObj)

	def doProcess(self):
		self._connServer()
		if self._orderSheet.get('version'):
			message = genVersionCheck(self._orderSheet)
		elif self._orderSheet.get('showGroups'):
			message = genNilmGroupInfo(self._orderSheet)
		elif self._orderSheet['workers']:
			message = genReqWorkersResources(self._orderSheet)
		elif self._orderSheet['outputCheck']:
			message = genReqOutputCheck(self._orderSheet)
		else:
			message = genOrderMessage(self._orderSheet)

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
			elif proto == 'RES_VERSION':
				versionInfo = recvMessage['versionInfo']
				if versionInfo.has_key('version'):
					version = versionInfo['version']
					print "# nilm cluster version : %s" %(version)
				if versionInfo.has_key('update_date'):
					updateDate = versionInfo['update_date']
					print " - updated date : %s" %(updateDate)
				if versionInfo.has_key('describe'):
					describe = versionInfo['describe']
					print " - describe : %s" %(describe)
				break
			elif proto == 'RES_GROUP_INFO':
				nilmGroupInfo = recvMessage['nilmGroupInfo']
				if not nilmGroupInfo:
					print "# Nilm Groups are not registered"
					break

				print "# Nilm Group Info"
				print "-" * 31
				print "%s%s : %s" %(' '*8, 'Group Name'.ljust(15), 'ID'.center(2))
				print "-" * 31
				gids = ['{0:0>3}'.format(gid) for gid in nilmGroupInfo.keys()]
				gids.sort()
				for gid in gids:
					groupName = str(nilmGroupInfo[int(gid)])
					print " - %s : %s" %(groupName.ljust(20), str(int(gid)).rjust(2))
				print "-" * 31
				break
			elif proto == 'RES_WORKERS_RESOURCES':
				workerResources = recvMessage['resoures']
				self._printWorkersResoures(workerResources)
				break
			elif proto == 'RES_OUTPUT_CHECK':
				outputStatusInfo = recvMessage['outputCheckInfo']
				self._printOutputStatus(outputStatusInfo)
				if self._orderSheet['sendMail']:
					self._printOutputStatusForMail(outputStatusInfo)
				break

	def _printWorkersResoures(self, workerResources):
		def printTotalProcesses(workerResources):
			if workerResources.has_key('resources'):
				totalCpu = 0
				idleCpu = 0
				runningCpu = 0
				waitCpu = 0
				numOfWorkers = len(workerResources['resources'])
				
				for workerId in workerResources['resources']:
					for processType in workerResources['resources'][workerId]:
						if processType == 'priority_runner':
							continue
						totalCpu += workerResources['resources'][workerId][processType]['total']
						idleCpu += workerResources['resources'][workerId][processType]['idle']
						runningCpu += workerResources['resources'][workerId][processType]['running']
						waitCpu += workerResources['resources'][workerId][processType]['wait']

				print "<< Total Process Status >>"
				print "=" * 100
				print "   DPU processes  |  workers : %s  |  total : %s,    idle : %s,    running : %s,    wait : %s" %(str(numOfWorkers).rjust(3), str(totalCpu).rjust(3), str(idleCpu).rjust(3), str(runningCpu).rjust(3), str(waitCpu).rjust(2))
				print "=" * 100

		def printWorkerStatus(workerResources):
			print "\n<< Workers Process Status >> "
			for resourceType in workerResources:
				if resourceType == 'resources':
					workerIds = workerResources[resourceType].keys()
					workerIds.sort()
					for workerId in workerIds:
						workerResource = workerResources[resourceType][workerId]
						priorityRunner = False
						if workerResource.has_key('priority_runner'):
							priorityRunner = workerResource['priority_runner']
						isFirst = True
						print "-" * 100
						for workerCpuType in workerResource.keys():
							if workerCpuType == 'priority_runner':
								continue
							if workerCpuType == 'privateCpu':
								processType = "Private Process"
							else:
								processType = "Public  Process"

							total = str(workerResource[workerCpuType]['total'])
							wait = str(workerResource[workerCpuType]['wait'])
							idle = str(workerResource[workerCpuType]['idle'])
							running = str(workerResource[workerCpuType]['running'])
							splitedWorkerId = workerId.split('_')
							workerAddr = splitedWorkerId[0]
							workerPort = splitedWorkerId[1]
							workerLoc = '%s:%s' %(workerAddr, workerPort)
							if isFirst:
								if priorityRunner:
									print " %s %s  |  %s  -  total : %s,   idle : %s,   running : %s,   wait : %s" %("*".ljust(1), workerLoc.ljust(17), processType.ljust(11), total.rjust(2), idle.rjust(2), running.rjust(2), wait.rjust(2))
								else:
									print " %s %s  |  %s  -  total : %s,   idle : %s,   running : %s,   wait : %s" %(" ".ljust(1), workerLoc.ljust(17), processType.ljust(11), total.rjust(2), idle.rjust(2), running.rjust(2), wait.rjust(2))
								isFirst = False
							else:
								print "    %s |  %s  -  total : %s,   idle : %s,   running : %s,   wait : %s" %(" ".ljust(17), processType.ljust(11), total.rjust(2), idle.rjust(2), running.rjust(2), wait.rjust(2))
			print "-" * 100
			print "\n"
					
		def printWaitQueueStatus(workerResources):
			print "\n<< DPU Wait Queue Status >>"
			print "-" * 100
			for resourceType in workerResources:
				if resourceType == 'workers' or resourceType == 'jobs':
					priorityQ = str(workerResources[resourceType]['priority'])
					privateQ = str(workerResources[resourceType]['private'])
					publicQ = str(workerResources[resourceType]['public'])
					if resourceType == 'workers':
						priorityEnabled = str(workerResources[resourceType]['priority_enabled'])
						print "  %s  -  Priority Q : %s,   Private Q : %s,   Public Q : %s  |   Priority Enabled : %s" %(resourceType.ljust(7), priorityQ.rjust(2), privateQ.rjust(4), publicQ.rjust(4), priorityEnabled.rjust(5))
					elif resourceType == 'jobs':
						priorityStack = str(workerResources[resourceType]['priority_stack'])
						print "  %s  -  Priority Q : %s,   Private Q : %s,   Public Q : %s  |   Priority Stack   : %s" %(resourceType.ljust(7), priorityQ.rjust(2), privateQ.rjust(4), publicQ.rjust(4), priorityStack.rjust(3))
			print "-" * 100

		printTotalProcesses(workerResources)
		printWaitQueueStatus(workerResources)
		printWorkerStatus(workerResources)

	def _printOutputStatus(self, outputStatusInfo):
		startTS = self._orderSheet['startTS']
		endTS = self._orderSheet['endTS']
		print "< Nilm Output Status (%s ~ %s) >" %(hdate(startTS), hdate(endTS))
		for gname in outputStatusInfo.keys():
			print '\n{0:-^58}'.format(" %s " %gname)
			sids = outputStatusInfo[gname].keys()
			sids.sort()
			for sid in sids:
				mainContext = " # %d  :  %s" %(sid, gname)
				self._logger.info(mainContext)
				bTSs = outputStatusInfo[gname][sid].keys()
				bTSs.sort()
				for bts in bTSs:
					isSuccess = outputStatusInfo[gname][sid][bts]['success']
					context = "     - %s (%d) - sucsessful : %s" %(hdate(bts), bts, str(isSuccess))
					if isSuccess:
						self._logger.info(context)
					else:
						self._logger.error(context)

	def _printOutputStatusForMail(self, outputStatusInfo):
		def sendMail(mailContents):
			mailSubject = "Nilm Failure Status (%s ~ %s)" %(hdate(self._orderSheet['startTS']), hdate(self._orderSheet['endTS']))
			if len(mailContents.split("\n")) == 1:
				mailContents += "\n\n Good!\n It did not fail NILM"
			MailSender().sendMail(mailSubject, mailContents)

		def cvtMailContents(predictionFailInfo):
			mailContents = []
			mailContents.append("< Nilm Failure Status (%s ~ %s) >" %(hdate(self._orderSheet['startTS']), hdate(self._orderSheet['endTS'])))
			for gname in predictionFailInfo.keys():
				mailContents.append('\n{0:-^38}'.format(" %s " %gname))
				sids = predictionFailInfo[gname].keys()
				sids.sort()
				for sid in sids:
					mailContents.append("\n # %d" %(sid))
					failedHdateList = predictionFailInfo[gname][sid]
					failedHdateList.sort()
					for failedHdate in failedHdateList:
						mailContents.append("     - %s" %(failedHdate))
			return "\n".join(mailContents)

		predictionFailInfo = {}
		for gname in outputStatusInfo.keys():
			sids = outputStatusInfo[gname].keys()
			sids.sort()
			for sid in sids:
				bTSs = outputStatusInfo[gname][sid].keys()
				for bts in bTSs:
					if outputStatusInfo[gname][sid][bts]['success']:
						continue

					basedHdate = "%s (%d)" %(hdate(bts), bts)
					if predictionFailInfo.has_key(gname):
						if predictionFailInfo[gname].has_key(sid):
							predictionFailInfo[gname][sid].append(basedHdate)
						else:
							predictionFailInfo[gname][sid] = [basedHdate]
					else:
						predictionFailInfo[gname] = {sid: [basedHdate]}
		mailContents = cvtMailContents(predictionFailInfo)
		sendMail(mailContents)


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
			self._logger.exception(e)

		protoMessage = pickle.loads("".join(dataBuffer))
		if self._debugMode:
			print " << received data : %s" %str(protoMessage)
		return protoMessage

	def close(self):
		self._socketObj.close()

class MailSender:
	def __init__(self):
		self._fromMailAddr, toMailAddrs, self._mailLib = getMailInfo()
		self._toMailAddrs = ",".join([toMailAddr for toMailAddr in toMailAddrs.replace(' ','').split(',')])

	def sendMail(self, subject, contents):
		from email.mime.text import MIMEText
		from subprocess import Popen, PIPE

		msg = MIMEText(contents)
		msg["Subject"] = subject
		msg["From"] = self._fromMailAddr.split(',')
		msg["To"] = self._toMailAddrs
		process = Popen([self._mailLib, "-t", "-oi"], stdin=PIPE)
		process.communicate(msg.as_string())

if __name__ == "__main__":
	NilmClient().doProcess()
