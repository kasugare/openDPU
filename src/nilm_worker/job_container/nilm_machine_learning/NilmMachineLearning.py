#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger
from common.conf_collector import getHdfsMlWeightDirPath, getLocalTmpPath, getHdfsMlTempDirPath
from error_code.ErrorCode import PathNotExistsError
from fs_handler.FSHandler import FSHandler
import signal
import inspect
import time
import sys
import os

PROCESS_NAME = 'NILM_MACHINE_LEARNING'

class NilmMachineLearning:
	def __init__(self, logger=None, debugMode='DEBUG'):
		if logger:
			self._logger = logger
		else:
			self._logger = Logger(PROCESS_NAME).getLogger()
		self._logger.setLevel(debugMode)
		self._fsHandler = FSHandler(logger)

	def __del__(self):
		self._logger.warn("- DEL NilmMachineLearning")

	def doProcess(self, processInfo, message):
		self._logger.info("########################################")
		self._logger.info("### DO Process Nilm Machine Learning ###")
		self._logger.info("########################################")

		processType = processInfo['processType']
		processIndex = processInfo['processIndex']

		sid = message['sid']
		countryCode = message['countryCode']
		timezone = message['timezone']
		threshold = message['threshold']
		numOfPoints = message['numOfPoints']
		formatOpt = message['formatOpt']

		trainingDataFilePath = message['trainingDataFilePath']
		savedOutputDirPath = message['savedOutputDirPath']
		modelName = message['modelName']
		dataFormat = message['dataFormat']
		weightFileName = message['weightFileName']

		localTempDirPath = getLocalTmpPath()
		hdfsWeightDirPath = getHdfsMlWeightDirPath()
		hdfsModelWeightFilePath = os.path.join(hdfsWeightDirPath, weightFileName)
		localModelWeightFilePath = os.path.join(localTempDirPath, weightFileName)

		try:
			if not os.path.exists(localModelWeightFilePath):
				self._fsHandler.getHdfsFileToLocal(hdfsModelWeightFilePath, localModelWeightFilePath)
		except Exception, e:
			self._logger.exception(e)
			raise PathNotExistsError
		
		self._logger.info("< %s >" %(sid))
		self._logger.info("-" * 100)
		self._logger.info("  - process type         : %s" %(processType))
		self._logger.info("  - process index        : %d" %(processIndex))
		self._logger.info("-" * 100)
		self._logger.info("  - countryCode          : %s" %(countryCode))
		self._logger.info("  - timezone             : %s" %(timezone))
		self._logger.info("  - threshold            : %0.2f" %(threshold))
		self._logger.info("  - numOfPoints          : %d" %(numOfPoints))
		self._logger.info("  - trainingDataFilePath : %s" %(trainingDataFilePath))
		self._logger.info("  - savedOutputDirPath   : %s" %(savedOutputDirPath))
		self._logger.info("  - modelName            : %s" %(modelName))
		self._logger.info("  - dataFormat           : %s" %(dataFormat))
		self._logger.info("  - weightFilePath       : %s" %(localModelWeightFilePath))
		self._logger.info("-" * 100)

		mlParams = {
			'sid': sid,
			'countryCode': countryCode,
			'timezone': timezone,
			'threshold': threshold,
			'numOfPoints': numOfPoints,
			'trainingDataFilePath': trainingDataFilePath,
			'savedOutputDirPath': savedOutputDirPath,
			'modelName': modelName,
			'dataFormat': dataFormat,
			'modelWeightFilePath': localModelWeightFilePath,
			'formatOpt': formatOpt
		}

		resultFilePath = self._runMlCore(processType, processIndex, **mlParams)

		return resultFilePath

	def _runMlCore(self, processType, processIndex, sid, countryCode, timezone, threshold, numOfPoints, trainingDataFilePath, savedOutputDirPath, modelName, dataFormat, modelWeightFilePath, formatOpt):
		self._logger.info("# process type : %s, processIndex : %d" %(processType, processIndex))
		if processType == 'GPU':
			os.environ["THEANO_FLAGS"] = "device=gpu%d,floatX=float32" %(processIndex)

		currentDirPath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
		corePath = os.path.join(currentDirPath, "ml_core/enlearner")
		sys.path.insert(0, corePath)

		import numpy as np
		import pandas as pd
		import model, data, helper

		data_model = data.DataManager(trainingDataFilePath, dataFormat, self._logger, {'p_length':numOfPoints, 'stat_info':formatOpt}).get_data()

		if data_model.raw_data.shape[0] < 1:
			resultFilePath = "/tmp/Non-Existing-Path/Input-Data-Is-Not-Valid"
			return resultFilePath
		
		model_to_test = model.ModelManager(modelName, self._logger, {'num_column':int(numOfPoints)}).get_model()
		model_to_test.fit(train_data=None, weights=modelWeightFilePath)
		test_set = data_model.generate_test_set(opt={})
		y = model_to_test.predict(test_set, opt={'verbose': 0})
		y = model.postprocess_time_merge(test_set['ts'].flatten(), y, threshold, 0.5)
		X_predicted = map(lambda x: 1 if x[1]>threshold else 0, np.array(y))

		resultFilePath = os.path.join(savedOutputDirPath, '%d_%d.x' %(sid, int(time.time()*1000)))
		out_data_temp = pd.DataFrame({'ts': test_set['ts'].flatten(), 'ap': map(lambda x: x*80.0,X_predicted), 'rp': [0.,]*len(X_predicted)})
		
		out_data = pd.DataFrame()
		from operator import add
		for t in out_data_temp.ts:
			one_row = out_data_temp[out_data_temp.ts==t]
			ap = float(one_row.ap)
			rp = float(one_row.rp)
			one_row_60s = pd.DataFrame({'timestamp':map(add,[t]*60,range(0,60)), 'active_power': [ap]*60, 'reactive_power':[rp]*60})
			out_data = pd.concat([out_data,one_row_60s])

		out_data = out_data[ ['timestamp','active_power','reactive_power'] ]
		out_data.to_csv(resultFilePath, index=False)
		
		self._logger.debug("# DL result : %s" %(str(resultFilePath)))
		return resultFilePath
