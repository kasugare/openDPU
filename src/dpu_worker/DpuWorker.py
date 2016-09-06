#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from dpu_worker.q_router.QRouter import QRouter
from dpu_worker.job_manager.JobWorkerManager import JobWorkerManager
from dpu_worker.job_deployer.JobDeployer import JobDeployer
from dpu_worker.resource_provider.ResourceProvider import ResourceProvider
from dpu_worker.network_manager.WorkerNetworkManager import WorkerNetworkManager
import multiprocessing
import random
import time

PROCESS_NAME = 'DPU_WORKER'

class DpuWorker:
	def __init__(self):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('# process start : %s' %PROCESS_NAME)
		self._workerId = "WK_%d%X" %(random.randrange(1,9)*random.randrange(100,999), int(time.time()*100000))
		self._dpuProcesses = []

	def _createMsgQueue(self):
		deployQ = {
			'reqQ' : multiprocessing.Queue(),
			'routerQ' : multiprocessing.Queue()
		}
		jobQ = {
			'reqQ' : multiprocessing.Queue(),
			'routerQ' : multiprocessing.Queue()
		}
		resourceQ = {
			'reqQ' : multiprocessing.Queue(),
			'routerQ' : multiprocessing.Queue()
		}
		networkQ = {
			'reqQ' : multiprocessing.Queue(),
			'routerQ' : multiprocessing.Queue()
		}
		return deployQ, jobQ, resourceQ, networkQ

	def runDpuWorker(self):
		deployQ, jobQ, resourceQ, networkQ = self._createMsgQueue()

		self._dpuProcesses.append(multiprocessing.Process(target=self._runJobDeployer, args=(deployQ,)))
		self._dpuProcesses.append(multiprocessing.Process(target=self._runJobWorker, args=(jobQ,)))
		self._dpuProcesses.append(multiprocessing.Process(target=self._runResourceProvider, args=(resourceQ,)))
		self._dpuProcesses.append(multiprocessing.Process(target=self._runNetworkManager, args=(networkQ,)))

		for process in self._dpuProcesses:
			process.daemon = True
			process.start()

		qRouter = QRouter(self._logger, self._dpuProcesses, deployQ, jobQ, resourceQ, networkQ)
		qRouter.doProcess(self._workerId)

	def _runJobDeployer(self, deployQ):
		JobDeployer(deployQ).doProcess()

	def _runJobWorker(self, jobQ):
		JobWorkerManager(jobQ).doProcess()

	def _runResourceProvider(self, resourceQ):
		ResourceProvider(resourceQ).doProcess()

	def _runNetworkManager(self, networkQ):
		WorkerNetworkManager(self._workerId, networkQ).doProcess()