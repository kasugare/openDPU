#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Worker ID
def genGetWidMsg(tempId):
	message = {
		'protocol': 'MW_NET_GET_WID',
		'statCode': 100,
		'tempId': tempId
	}
	return message

def genSetWid(tempID, workerId):
	message = {
		'protocol': 'WM_NET_SET_WID',
		'statCode': 200,
		'result': {
			'tempId': tempID,
			'workerId': workerId
		}
	}
	return message

def genErrorWid(tempId, errorMsg):
	message = {
		'protocol': 'WM_NET_SET_WID',
		'statCode': 500,
		'tempId': tempId,
		'error': str(errorMsg)
	}
	return message

# Heart Beat
def genReqHeartBeat(workerId):
	message = {
		'protocol': 'MW_NET_STAT_HB',
		'statCode': 100,
		'workerId': workerId
	}
	return message

def genResHeartBeat(workerId, error=None):
	message = {
		'protocol': 'WM_NET_STAT_HB',
		'statCode': 200,
		'workerId': workerId
	}
	if error:
		message['statCode'] = 500
		message['error'] = error
	return message

# Worker Resource
def genGetWorkerResources(workerId):
	message = {
		'protocol': 'MW_RS_GET_RESOURCE',
		'statCode': 100,
		'workerId': workerId
	}
	return message

def genSetWorkerResource(workerId, resources):
	message = {
		'protocol': 'WM_RS_SET_RESOURCE',
		'statCode': 200,
		'workerId': workerId,
		'result': resources
	}
	return message