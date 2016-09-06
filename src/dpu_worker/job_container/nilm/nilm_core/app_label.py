#!/usr/bin/env python
# -*- coding: utf-8 -*-

class BaseAppLabel:
	def __init__(self):
		pass

	def find_app_type(self, app):
		raise NotImplementedError


class SimpleRuleAppLabel(BaseAppLabel):
	def __init__(self, _logger):
		self._logger = _logger

	def find_app_type(self, app):
		app_shape = app['shape_type']
		self._logger.info("# Find nilm appliance types, appliance shape : %s" %(app_shape))

		if( app_shape == 'TV' ):
			return 12
		if( app_shape == 'ricecooker_pattern_scan' ):
			return 66
		elif( app_shape == 'HMM' ):
			return 65
		if( app_shape == 'WashingMachine' ):
			return 67
		elif( app_shape == 'pattern_scan' ):
			ap = app['rising_edge']['str2end.active_power_med']
			rp = app['rising_edge']['str2end.reactive_power_med']
			on_time = app['falling_edge']['EffTimeOn.med']
			if( ap > 800 ):
				return 69
			if( on_time < (60*5) ):
				return 63
			else:
				return 62

		elif( app_shape == 'cyclic_box' ):
			ap = app['rising_edge']['str2end.active_power_med']
			rp = app['rising_edge']['str2end.reactive_power_med']
			on_time = app['cycle']['working_time']

			if( ap > 800 ):
				return 69
			if( on_time < (60*5) ):
				return 63
			else:
				return 62
		elif( app_shape == 'high_power' ):
			return 69
		elif( app_shape == 'pattern_scan_heavy' ):
			return 69
		elif( app_shape == 'StandbyPower' ):
			return 70
		else:
			return 71


if __name__ == '__main__':
	import json

	app = """
{
		"meta-version" : 1,
		"shape_type" : "high_power",
		"rising_edge" : {
			"ap.re.mu" :1358.9,
			"ap.re.sigma" : 15.346,
			"rp.re.mu" : 27.22,
			"rp.re.sigma" : 33.6
		},
		"falling_edge" : {
			"ap.fe.mu" : -1350.7,
			"ap.fe.sigma" : 15.236,
			"rp.fe.mu" : -67.99,
			"rp.fe.sigma" : 28.427
		},
		"box.no" : 10,
		"generation_info" : {
			"data_used" : {
				"start" : "2015-06-02",
				"end" : "2015-06-06 23:14:48",
				"sampling" : 1
			},
			"computed" : 1.4384e+09
		}
	}"""

	app_info = {"1":json.loads(app)}
	app_label = SimpleRuleAppLabel()
	print app_label.find_app_type(app_info['1'])


