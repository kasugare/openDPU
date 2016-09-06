#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import math

class BaseAppInfoComp:
	def __init__(self, _logger):
		self._logger = _logger

	"""
	info:
		* keys = virtual feeder ids
		* values = json configuration of virtual feeder
	"""
	def set_current_app_info(self, info):
		self._logger.info("# Set current appliance info")
		self.current_info = info

	"""
	new_app: json configuration

	"""
	def find_similar_app(self, new_app):
		raise NotImplementedError


class AppDistanceComp(BaseAppInfoComp):
	def __init__(self, _logger):
		BaseAppInfoComp.__init__(self, _logger)
		self._logger = _logger
		self.tolerance = 0.15

	def find_similar_app(self, new_app):
		self._logger.info("# Find similar appliance meta")
		min_dist = 1.0
		min_app = None

		for k in self.current_info.keys():
			app = self.current_info[k]
			dist = self.compute_distance(app, new_app)
			if( dist <= min_dist ):
				min_app = k
				min_dist = dist

		if( min_app != None and min_dist < self.tolerance ):
			return min_app

	def compute_distance(self, app1, app2):
		self._logger.info("# Compute meta distance, src shape type : %s, tar shape type : %s" %(app1['shape_type'], app2['shape_type']))
		if( app1['shape_type'] != app2['shape_type'] ):
			return 1.
		elif( app1['shape_type'] == 'cyclic_box' ):
			rap1 = app1['rising_edge']['str2end.active_power_med']
			rap2 = app2['rising_edge']['str2end.active_power_med']

			rrp1 = app1['rising_edge']['str2end.reactive_power_med']
			rrp2 = app2['rising_edge']['str2end.reactive_power_med']

			fap1 = app1['falling_edge']['str2end.active_power_med']
			fap2 = app2['falling_edge']['str2end.active_power_med']

			frp1 = app1['falling_edge']['str2end.reactive_power_med']
			frp2 = app2['falling_edge']['str2end.reactive_power_med']

			t1 = app1['cycle']['working_time']
			t2 = app2['cycle']['working_time']

			app1_dist = math.sqrt(rap1**2 + rrp1**2 + fap1**2 + frp1**2)
			dist = math.sqrt((rap1-rap2)**2 + (rrp1-rrp2)**2 + (fap1-fap1)**2 + (frp1-frp2)**2)
			return dist/app1_dist

		elif( app1['shape_type'] == 'high_power' ):
			rap1 = 0.0
			rap2 = 0.0

			rrp1 = 0.0
			rrp2 = 0.0

			fap1 = 0.0
			fap2 = 0.0

			frp1 = 0.0
			frp2 = 0.0

			# to support old variable name
			try:
				rap1 = app1['rising_edge']['str2end.active_power_med']
				rap2 = app2['rising_edge']['str2end.active_power_med']

				rrp1 = app1['rising_edge']['str2end.reactive_power_med']
				rrp2 = app2['rising_edge']['str2end.reactive_power_med']

				fap1 = app1['falling_edge']['str2end.active_power_med']
				fap2 = app2['falling_edge']['str2end.active_power_med']

				frp1 = app1['falling_edge']['str2end.reactive_power_med']
				frp2 = app2['falling_edge']['str2end.reactive_power_med']
			except:
				pass

			app1_dist = math.sqrt(rap1**2 + rrp1**2 + fap1**2 + frp1**2)
			dist = math.sqrt((rap1-rap2)**2 + (rrp1-rrp2)**2 + (fap1-fap1)**2 + (frp1-frp2)**2)
			return dist/app1_dist
		else:
			return 1.0




if __name__ == '__main__':

	app_info_one = """
{
		"meta-version" : 1,
		"shape_type" : "cyclic_box",
		"rising_edge" : {
			"str2end.active_power_med" : 124.39,
			"ap_sigma" : 121.32,
			"str2end.reactive_power_med" : -232.87,
			"rp_sigma" : 6.7989
		},
		"falling_edge" : {
			"str2end.active_power_med" : -137.43,
			"ap_sigma" : 115.96,
			"str2end.reactive_power_med" : 227.97,
			"rp_sigma" : 9.5789
		},
		"cycle" : {
			"working_time" : 242.77,
			"working_time_sigma" : 102.04,
			"duty_cycle" : 0.14119
		},
		"box.no" : 60,
		"generation_info" : {
			"data_used" : {
				"start" : "2015-06-02",
				"end" : "2015-06-06 23:14:48",
				"sampling" : 1
			},
			"computed" : 1.4384e+09
		}
	}"""

	app_info_two = """
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
	app_info_one_1 = """
{
		"meta-version" : 1,
		"shape_type" : "cyclic_box",
		"rising_edge" : {
			"str2end.active_power_med" : 11.39,
			"ap_sigma" : 121.32,
			"str2end.reactive_power_med" : -232.87,
			"rp_sigma" : 6.7989
		},
		"falling_edge" : {
			"str2end.active_power_med" : -137.43,
			"ap_sigma" : 115.96,
			"str2end.reactive_power_med" : 227.97,
			"rp_sigma" : 9.5789
		},
		"cycle" : {
			"working_time" : 242.77,
			"working_time_sigma" : 102.04,
			"duty_cycle" : 0.14119
		},
		"box.no" : 60,
		"generation_info" : {
			"data_used" : {
				"start" : "2015-06-02",
				"end" : "2015-06-06 23:14:48",
				"sampling" : 1
			},
			"computed" : 1.4384e+09
		}
	}"""

	app_info_two_1 = """
{
		"meta-version" : 1,
		"shape_type" : "high_power",
		"rising_edge" : {
			"ap.re.mu" :1358.1,
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

	app_info = {"1":json.loads(app_info_one),"2":json.loads(app_info_two)}
	app_comp = AppDistanceComp()
	app_comp.set_current_app_info(app_info)
	print app_comp.find_similar_app(json.loads(app_info_two_1))


