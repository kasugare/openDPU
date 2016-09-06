#!/usr/bin/env python
# -*- coding: utf-8 -*-

from app_detect import *
import pandas

meta_detector = AppDetectR()

test_nilm_data_names = ['timestamp','voltage','current','active_power','reactive_power','apparent_power','power_factor']
test_nilm_data = pandas.read_csv('./data/nilm_input_test.csv',names=test_nilm_data_names)
meta_detector.set_data(test_nilm_data)
print 'read complete...'
if meta_detector.detect_apps():
	print meta_detector.get_app_info()
