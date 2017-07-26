# -*- coding: utf-8 -*-

import ConfigParser
import traceback
import sys
import os

CONF_PATH = "../conf/"
CONF_FILENAME = "country_target.meta"

COUNTRY_INFO_LIST = ('device_channel', 'device_frequency', 'nilm_frequency', 'timezone')

def getConfig():
	src_path = os.path.dirname(CONF_PATH)
	ini_path = src_path + "/" + CONF_FILENAME
	if not os.path.exists(ini_path):
		print "# Cannot find country_target.meta in conf directory : %s" %src_path
		sys.exit(1)

	conf = ConfigParser.RawConfigParser()
	conf.read(ini_path)
	return conf

def getCountryDeviceMetaInfos():
	countryDeviceMetas = {}
	conf = getConfig()

	for section in conf.sections():
		for elementName in COUNTRY_INFO_LIST:
			if elementName == 'timezone':
				elementValue = conf.get(section, elementName).replace(' ','').split(',')
			else:
				elementValue = int(conf.get(section, elementName))
			if countryDeviceMetas.has_key(section):
				countryDeviceMetas[section][elementName] = elementValue
			else:
				countryDeviceMetas[section] = {elementName : elementValue}
	return countryDeviceMetas
