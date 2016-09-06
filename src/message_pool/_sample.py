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

# Data Protocol
def genReqNilm(params):
    message = {
        'proto': 'REQ_NILM',
        'params': params
    }
    return message

def genReqNilmData(collectorType, deviceType, jobType, offerType, params={}):
    message = {
        'proto': 'REQ_NILM_DATA',
        'collector': collectorType,
        'device': deviceType,
        'jobType': jobType,
        'offer': offerType,
        'params': params
    }
    return message

def genResNilmData(jobType, offerType, result, dataSet='SINGLE'):
    message = {
        'proto': 'RES_NILM_DATA',
        # 'dataset': dataSet,
        'jobType': jobType,
        'offer': offerType,
        'result': result
    }
    return message

# Resource Status
def genReqResourceStat(availCpu):
    message = {
        'proto': 'REQ_INIT_RESOURCE',
        'availCpu':availCpu
    }
    return message

def genReqJobData(jobName, params):
    message = {
        'proto': 'REQ_RUN_JOB',
        'jobName': jobName,
        'params': params
    }
    return message

def getReqJobCompelted(jobId, resource):
    message = {
        'proto': 'REQ_JOB_DONE',
        'resource': resource
    }
    return message

def genReqJobCompelted(jobId, availCpu, result=None):
    message = {
        'proto': 'REQ_JOB_SUCCES',
        'availCpu': availCpu,
        'result': result
    }
    return message

def genReqJobFail(jobId, availCpu, message, error=''):
    message = {
        'proto': 'REQ_JOB_FAIL',
        'availCpu': availCpu,
        'jobMsg': message,
        'error': error
    }
    return message