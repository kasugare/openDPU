#!/usr/bin/env python
# -*- coding: utf-8 -*-

class RequestHandler:
    def __init__(self, logger, clientQ, socketObj, addr):
        self._logger = logger
        self._reqQ = clientQ['reqQ']
        self._routerQ = clientQ['routerQ']
