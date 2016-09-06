#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Worker ID
def genSetSysWidMsg(workerId):
	message = {
		'protocol': 'SYS_SET_WID',
		'statCode': 100,
		'workerId': workerId
	}
	return message

def genGetSysWidMsg(tempId, workerId = None):
	message = {
		'protocol': 'SYS_GET_WID',
		'statCode': 100,
		'tempId': tempId
	}
	if workerId:
		message['workerId'] = workerId
	return message

def genConnectionLost(workerId, error=None):
	message = {
		'protocol': 'SYS_CONN_CLOSE',
		'statCode': 100,
		'workerId': workerId
	}
	if error:
		message['statCode'] = 503
		message['error'] = error
	return message

def genGetWorkerResource(workerId = None):
	message = {
		'protocol': 'SYS_REQ_RESOURCE',
		'statCode': 100
	}
	if workerId:
		message['workerId'] = workerId
	return message

def getSetWorkerResource(workerId, resources):
	message = {
		'protocol': 'SYS_SET_RESOURCE',
		'workerId': workerId,
		'statCode': 100,
		'resource': resources
	}
	return message

def genResWorkerResource(workerId, resources):
	message = {
		'protocol': 'SYS_RES_RESOURCE',
		'statCode': 200,
		'workerId': workerId,
		'result': resources
	}
	return message

def genDelWorkerResource(workerId):
	message = {
		'protocol': 'SYS_DEL_RESOURCE',
		'statCode': 200,
		'workerId': workerId
	}
	return message