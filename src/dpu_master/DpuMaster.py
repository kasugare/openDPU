#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from dpu_master.q_router.QRouter import QRouter
from dpu_master.client_listener.ClientListener import ClientListener
from dpu_master.job_manager.JobManager import JobManager
from dpu_master.deploy_manager.DeployManager import DeployManager
from dpu_master.resource_manager.ResourceMaster import ResourceMaster
from dpu_master.network_manager.MasterNetworkManager import MasterNetworkManager
import multiprocessing

PROCESS_NAME = 'DPU_MASTER'

class DpuMaster:
	def __init__(self):
		self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.info('# process start : %s' %PROCESS_NAME)
		self._dpuProcesses = []

	def _createMsgQueue(self):
		clientQ = {
			'reqQ' : multiprocessing.Queue(),
			'routerQ' : multiprocessing.Queue()
		}
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
		return clientQ, deployQ, jobQ, resourceQ, networkQ

	def runDpuMaster(self):
		clientQ, deployQ, jobQ, resourceQ, networkQ = self._createMsgQueue()

		self._dpuProcesses.append(multiprocessing.Process(target=self._runClientListener, args=(clientQ,)))
		self._dpuProcesses.append(multiprocessing.Process(target=self._runDeployManager, args=(deployQ,)))
		self._dpuProcesses.append(multiprocessing.Process(target=self._runJobManager, args=(jobQ,)))
		self._dpuProcesses.append(multiprocessing.Process(target=self._runResourceMaster, args=(resourceQ,)))
		self._dpuProcesses.append(multiprocessing.Process(target=self._runNetworkManager, args=(networkQ,)))

		for process in self._dpuProcesses:
			process.daemon = True
			process.start()

		qRouter = QRouter(self._logger, self._dpuProcesses, clientQ, deployQ, jobQ, resourceQ, networkQ)
		qRouter.doProcess()

	def _runClientListener(self, clientQ):
		ClientListener(clientQ).runServer()

	def _runDeployManager(self, deployQ):
		DeployManager(deployQ).doProcess()

	def _runJobManager(self, jobQ):
		JobManager(jobQ).doProcess()

	def _runResourceMaster(self, resourceQ):
		ResourceMaster(resourceQ).doProcess()

	def _runNetworkManager(self, networkQ):
		MasterNetworkManager(networkQ).doProcess()