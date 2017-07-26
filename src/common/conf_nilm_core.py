#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import sys
import os

CONF_PATH = "../conf/"
CONF_FILENAME = "nilm_core.ini"

def getConfig():
    src_path = os.path.dirname(CONF_PATH)
    ini_path = src_path + "/" + CONF_FILENAME
    if not os.path.exists(ini_path):
        print "# Cannot find nilm_core.ini in conf directory : %s" %src_path
        sys.exit(1)

    conf = ConfigParser.RawConfigParser()
    conf.read(ini_path)
    return conf

def getNilmCorePath(confName = 'NILM_CORE'):
    conf = getConfig()
    nilmCorePath = conf.get(confName, 'core_path').split(',')[0]
    return nilmCorePath