#!/usr/bin/env python
# -*- coding: utf-8 -*-

def parseProtocol(message):
	if not message or not message.has_key('protocol') or not message.has_key('statCode'):
		raise ValueError
	protocol = message['protocol']
	statCode = message['statCode']
	return protocol, statCode
