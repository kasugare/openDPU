#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dpu_master.DpuMaster import DpuMaster
from dpu_worker.DpuWorker import DpuWorker
import sys

if __name__ == '__main__':
	# DpuMaster().runDpuMaster()

	if len(sys.argv) != 2:
		print "[error] incorrected params", sys.argv
		sys.exit(1)
	else:
		mode = sys.argv[1]
		if mode == 'master':
			DpuMaster().runDpuMaster()
		elif mode == 'worker':
			DpuWorker().runDpuWorker()
		else:
			print "[error] incorected running mode."
			DpuWorker().runDpuWorker()

