#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common_utility import getHourlyDatetime, getDailyDatetime, getPeriodTS
from optparse import OptionParser
from datetime import datetime
import traceback
import copy
import sys
import os

class UserOptionParser:
    def __init__(self):
        self._options()

    def _options(self):
        usage = """usage: %prog [options] arg1,arg2 [options] arg1
$ python NilmCluster.py -v -p 2015-01-01_00:00:00 2015-01-02_23:59:59 -s 1 -D edm3_090"""

        parser = OptionParser(usage = usage)
        parser.add_option("-v", "--version",
            action="store_true",
            default=False,
            dest="version",
            help="""get version of the Nilm Cluster
            [use] -v or --version""")

        parser.add_option("-s", "--sids",
            action="store",
            type="str",
            dest="sids",
            nargs=1,
            default="*",
            help="""generated data by a site id. It will be create only one site id.
            [use] -s 1 or  --sid=1
            [options] -s 1,2,3 or not
            [default: %default]""")

        parser.add_option("--RESOURCES",
            action="store",
            dest="workers",
            default=None,
            help="""get resources of workers on openDPU
            [use] --RESOURCES
            [options] --RESOURCES or None
            [default: %default""")

        parser.add_option("--DEBUG",
            action="store_true",
            default=False,
            dest="debugMode",
            help="""[use] --DEBUG
            [options] --DEBUG or None
            [default: %default""")

        options, args = parser.parse_args()
        self._vaildOptions(options, args)

    def _vaildOptions(self, options, args):
        optVaildator = OptionValidator(options, args)
        self.userOptions = optVaildator.doCheckOptions()

    def getUserOption(self):
        debugMode = self.userOptions.get('debugMode')
        if debugMode:
            print "-" * 50
            for optKey in self.userOptions:
                print '- %s : %s' %(optKey.ljust(15), str(self.userOptions[optKey]))
            print "-" * 50
        return self.userOptions


class OptionValidator:
    def __init__(self, options, args):
        self._userOptions = self._setUserOptions(options, args)

    def _setUserOptions(self, options, args):
        user_options = {}
        if options.version != None:
            user_options['version'] = options.version
        if options.sids != None:
            user_options['sids'] = options.sids.split(',')
        if options.workers != None:
            user_options['workers'] = options.workers
        if options.debugMode != None:
            user_options['debugMode'] = options.debugMode
        return user_options

    def doCheckOptions(self):
        userOptions = {}
        userOptions['version'] = self._userOptions.get('version')
        userOptions['sids'] = self._checkSiteId()
        userOptions['workers'] = self._checkWorkerResoures()
        userOptions['debugMode'] = self._userOptions.get('debugMode')
        return userOptions

    def _checkSiteId(self):
        tarSids = []
        sids = self._userOptions.get('sids')
        def isNumber(string):
            try:
                int(string)
                return True
            except ValueError:
                return False

        for sid in sids:
            if isNumber(sid):
                pass
            else:
                if sid == '*':
                    return ['*']
                else:
                    print "# Wrong site ids in the site id. Device id will be integer number or '*'."
                    sys.exit(1)
        return [int(sid) for sid in sids]

    def _checkWorkerResoures(self):
        workers = self._userOptions.get('workers')
        if "*" == workers:
            workers = "all"
            return workers
        elif "all" == workers:
            return workers
        elif not workers:
            return None
        else:
            print "# Wrong workers resources options"
            sys.exit(1)
