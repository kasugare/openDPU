#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common.util_logger import Logger

PROCESS_NAME = 'DATA_ETL'

class DataEtlManager:
    def __init__(self, logger=None, debugMode='DEBUG'):
        if logger:
            self._logger = logger
        else:
            self._logger = Logger(PROCESS_NAME).getLogger()
        self._logger.setLevel(debugMode)

    def doProcess(self):
        self._logger.info("###########################")
        self._logger.info("### DO Process Data Etl ###")
        self._logger.info("###########################")

        # SchedulingHandler(self._logger).doProcess()

if __name__ == '__main__':
    etlManager = DataEtlManager()
    etlManager.doProcess()
