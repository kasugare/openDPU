#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Heart beat
def genReqHB():
	message = {'proto': 'REQ_HB'}
	return message

def genResHB():
	message = {'proto': 'RES_HB'}
	return message

def genReqHBResource():
	message = {'proto': 'REQ_HB_RES_STAT'}
	return message

def genResHBResource():
	message = {'proto': 'RES_HB_RES_STAT'}
	return message

def genReqWorkersResources():
	message = {
		'proto': 'REQ_WORKERS_RESOURCES'
	}

def genResWorkersResources(workerResourcesMessage):
	message = {
		'proto': 'RES_WORKERS_RESOURCES',
		'resoures': workerResourcesMessage
	}
	return message

# Version Check
def genResVersion(versionInfo):
	message = {
		'proto': 'RES_VERSION',
		'versionInfo': versionInfo
	}
	return message

# Job Complete
def genReqJobCompleted():
	message = {'proto': 'REQ_JC'}
	return message

# Connection Close
def genReqConnClose():
	message = {'proto': 'REQ_CC'}
	return message

def genResConnClose():
	message = {'proto': 'RES_CC'}
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


# Progressive Message
def genReqProgressMessage(progress):
	message = {'proto': 'REQ_PROGS', 'message': progress}
	return message


# System status
def genReqStatus():
	message = {'proto': 'REQ_STAT'}
	return message

def genResStatus(processRate, totalMem, usedMem, jobs):
	message = {
		'proto': 'REQ_STAT',
		'stat': {
			'process':processRate,
			'totalMem':totalMem,
			'used_memory': usedMem,
			'jobs':jobs
		}
	}
	return message

# Job Run
def genReqJobRun(params):
	message = {
		'proto': 'REQ_DPU_JOB',
		'params': params
	}
	return message

def getReqEtlJob(jobType, params={}):
	message = {
		'proto': 'REQ_ETL_JOB',
		'jobType': jobType,
		'params': params,
		'retry': 0,
		'processType': None
	}
	return message


# Data Protocol
def genReqDpuRawData(params):
	message = {
		'proto': 'REQ_DPU_RAW_DATA',
		'params': params
	}
	return message

def genReqDpuStatusCheck(collectorType, jobType, params={}):
	message = {
		'proto': 'REQ_DPU_STATUS_CHECK',
		'collector': collectorType,
		'jobType': jobType,
		'retry': 0,
		'params': params
	}
	return message

def genReqGenDpuRawData(collectorType, deviceType, jobType, offerType, params={}):
	message = {
		'proto': 'REQ_GEN_DPU_RAW_DATA',
		'collector': collectorType,
		'device': deviceType,
		'jobType': jobType,
		'offer': offerType,
		'retry': 0,
		'params': params
	}
	return message

def genReqDpuLearnerSchedule(jobType, params={}):
	message = {
		'proto': 'REQ_DPU_LEARN_SCHEDULE',
		'jobType': jobType,
		'params': params,
		'retry': 0,
		'processType': None
	}
	return message


def genReqDpuData(collectorType, deviceType, jobType, offerType, params={}):
	message = {
		'proto': 'REQ_DPU_DATA',
		'collector': collectorType,
		'device': deviceType,
		'jobType': jobType,
		'offer': offerType,
		'retry': 0,
		'params': params
	}
	return message

def genResDpuData(jobType, offerType, result, dataSet='SINGLE'):
	message = {
		'proto': 'RES_DPU_DATA',
		# 'dataset': dataSet,
		'jobType': jobType,
		'offer': offerType,
		'result': result
	}
	return message

# Resource Status
def genReqResourceStat(totalCpu, publicCpu):
	message = {
		'proto': 'REQ_INIT_RESOURCE',
		'totalCpu': totalCpu,
		'publicCpu': publicCpu
	}
	return message

def genReqJobData(jobName, params):
	message = {
		'proto': 'REQ_RUN_JOB',
		'jobName': jobName,
		'params': params
	}
	return message

def genReqTajoEnable(jobType):
	message = {
		'proto': 'REQ_TAJO_ENABLE',
		'jobType': jobType
	}
	return message

def genReqJobCompelted(jobId, availCpu, jobType = None, result=None, processType=None):
	message = {
		'proto': 'REQ_JOB_SUCCES',
		'availCpu': availCpu,
		'jobType' : jobType,
		'result': result,
		'processType': processType
	}
	return message

def genReqJobFail(jobId, availCpu, message, error='', processType=None):
	message = {
		'proto': 'REQ_JOB_FAIL',
		'availCpu': availCpu,
		'jobMsg': message,
		'error': error,
		'processType': processType
	}
	return message
