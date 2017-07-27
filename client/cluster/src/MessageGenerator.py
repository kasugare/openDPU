#!/usr/bin/env python
# -*- coding: utf-8 -*-

def genOrderMessage(orderSheet):
	message = {
		'proto': 'REQ_DPU_JOB',
		'orderSheet': {
			'jobs': orderSheet['jobs'],
			'storage': orderSheet['storage'],
			'paths': orderSheet['paths'],
			'version': orderSheet['version'],
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

def genReqWorkersResources(orderSheet):
	message = {
		'proto': 'REQ_WORKERS_RESOURCES',
		'orderSheet': {
			'workers': [orderSheet['workers']]
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
