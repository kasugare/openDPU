#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dpu_master.DpuClusterMaster import DpuClusterMaster
from dpu_worker.DpuClusterWorker import DpuClusterWorker
import sys

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "[error] incorrected params", sys.argv
		sys.exit(1)
	else:
		mode = sys.argv[1]
		if mode == 'master':
			DpuClusterMaster().doProcess()
		elif mode == 'worker':
			DpuClusterWorker().doProcess()
		else:
			print "[error] incorected running mode."

