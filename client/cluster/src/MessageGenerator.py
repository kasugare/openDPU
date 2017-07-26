#!/usr/bin/env python
# -*- coding: utf-8 -*-

def genOrderMessage(orderSheet):
	message = {
		'proto': 'REQ_NILM_JOB',
		'orderSheet': {
			'remove': orderSheet['remove'],
			'groupNames': orderSheet['groupNames'],
			'sids': orderSheet['sids'],
			'startTS': orderSheet['startTS'],
			'endTS': orderSheet['endTS'],
			'jobs': orderSheet['job'],
			'forced': orderSheet['userInputPeriod'],
			'version': orderSheet['version'],
			'rExtOpt': orderSheet['rExtOpt'],
			'debugMode': orderSheet['debugMode']
			}
		}
	return message

def genVersionCheck(orderSheet):
	message = {
		'proto': 'REQ_VERSION',
		'orderSheet': {
			'version': orderSheet['version']
			}
		}
	return message

def genNilmGroupInfo(orderSheet):
	message = {
		'proto': 'REQ_GROUP_INFO',
		'orderSheet': {
			'nilmGroupInfo': orderSheet['showGroups']
			}
		}
	return message

def genReqWorkersResources(orderSheet):
	message = {
		'proto': 'REQ_WORKERS_RESOURCES',
		'orderSheet': {
			'workers': [orderSheet['workers']]
		}
	}
	return message

def genReqOutputCheck(orderSheet):
	message = {
		'proto': 'REQ_OUTPUT_CHECK',
		'orderSheet': {
			'groupNames': orderSheet['groupNames'],
			'sids': orderSheet['sids'],
			'startTS': orderSheet['startTS'],
			'endTS': orderSheet['endTS']
			}
	}
	return message

# OK
def genResOK():
	message = {'proto': 'RES_OK'}
	return message

# ErrorgenProgressMessage
def genReqError(error):
	message = {'proto': 'REQ_ERR', 'error': str(error)}
	return message

def genResError(error):
	message = {'proto': 'RES_ERR', 'error': str(error)}
	return message
