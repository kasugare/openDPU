#!/usr/bin/env python
# -*- coding: utf-8 -*-

def genResourceInfoMessage():
	message = {
		'protocol': 'CM_REQ_ALL_RESOURCE_INFO',
		'statCode': 100
	}
	return message

def genOrderMessage(orderSheet):
	message = {
		'proto': 'REQ_NILM_JOB',
		'orderSheet': {
			'remove': orderSheet['remove'],
			'gids': orderSheet['gids'],
			'sids': orderSheet['sids'],
			'startTS': orderSheet['startTS'],
			'endTS': orderSheet['endTS'],
			'jobs': orderSheet['job']
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