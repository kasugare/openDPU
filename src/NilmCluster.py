#!/usr/bin/env python
# -*- coding: utf-8 -*-

from nilm_master.NilmClusterMaster import NilmClusterMaster
from nilm_worker.NilmClusterWorker import NilmClusterWorker
import sys

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "[error] incorrected params", sys.argv
		sys.exit(1)
	else:
		mode = sys.argv[1]
		if mode == 'master':
			NilmClusterMaster().doProcess()
		elif mode == 'worker':
			NilmClusterWorker().doProcess()
		else:
			print "[error] incorected running mode."

