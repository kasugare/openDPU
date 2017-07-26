#!/usr/bin/env python
# -*- coding: utf-8 -*-

from rainbow_logging_handler import RainbowLoggingHandler
import logging.handlers
import logging
import sys


class Logger:
	def __init__(self):
		logger = logging.getLogger("Client")
		logger.setLevel("DEBUG")
		loggerFormat = " %(message)s"
		formatter = logging.Formatter(loggerFormat)

		isFile = False
		isStream = True

		if isFile:
			isLotate = 10
			dirPath = loggerInfo['log_dir']
			fileName = loggerInfo['log_filename']
			maxBytes = loggerInfo['log_maxsize']
			backupCnt = loggerInfo['backup_count']
			handler = self._setFileLoggingHandler(dirPath, fileName, formatter, isLotate, maxBytes, backupCnt)
			logger.addHandler(handler)
		if isStream:
			handler = self._setStreamLoggingHandler(formatter)
			logger.addHandler(handler)
		self.logger = logger

	def _getLevel(self, level = "INFO"):
		level = level.upper()
		if level == "DEBUG":
			return logging.DEBUG
		elif level == "ERROR":
			return logging.ERROR
		elif level == "FATAL":
			return logging.FATAL
		elif level == "CRITICAL":
			return logging.CRITICAL
		elif level == "WARN":
			return logging.WARN
		else:
			return logging.INFO

	def _setFileLoggingHandler(self, dirPath, fileName, formatter, isLotate=True, maxBytes=104857600, backupCnt=10):
		filePath = '%s/%s' %(dirPath, fileName)

		if isLotate:
			handler = logging.handlers.RotatingFileHandler(filePath, maxBytes=maxBytes, backupCount=backupCnt)
		else:
			handler = logging.FileHandler(filePath)
		handler.setFormatter(formatter)
		return handler

	def _setStreamLoggingHandler(self, formatter):
		handler = RainbowLoggingHandler(
			sys.stderr,
			datefmt 				= '%Y-%m-%d %H:%M:%S',
			color_name				= ('white'	, None, False),
			color_levelno			= ('white'	, None, False),
			color_levelname			= ('white'	, None, False),
			color_pathname			= ('blue'	, None, True),
			color_filename			= ('blue'	, None, True),
			color_module			= ('blue'	, None, True),
			color_lineno			= ('cyan'	, None, True),
			color_funcName			= ('blue'	, None, True),
			color_created			= ('white'	, None, False),
			color_asctime			= ('black'	, None, True),
			color_msecs				= ('white'	, None, False),
			color_relativeCreated	= ('white'	, None, False),
			color_thread			= ('white'	, None, False),
			color_threadName		= ('white'	, None, False),
			color_process			= ('black'	, None, True),
			color_message_debug		= ('cyan'	, None , False),
			color_message_info		= ('white'	, None , False),
			color_message_warning	= ('yellow'	, None , True),
			color_message_error		= ('red'	, None , True),
			color_message_critical	= ('white'	, 'red', True))
		handler.setFormatter(formatter)
		return handler

	def setLevel(self, level = "INFO"):
		loggerLevel = self._getLevel(level)
		logger.setLevel(loggerLevel)

	def getLogger(self):
		return self.logger
