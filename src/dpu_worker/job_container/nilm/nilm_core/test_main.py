from app_detect import *
from app_daily_load import *
import pandas

meta_detector = AppDetectR()

#test_nilm_data_names = ['timestamp','voltage','current','active_power','reactive_power','apparent_power','power_factor']
#test_nilm_data = pandas.read_csv('./data/nilm_input_test.csv',names=test_nilm_data_names)
datafile = '/home/nilm/analytics_master/NilmCluster/data/10000358(3)-2015.07.24~2015.07.31.csv'
datafile = '/home/nilm/analytics_master/NilmCluster/data/10000358(3)-2015.08.01~2015.08.02.csv'
datafile = '/home/nilm/analytics_master/NilmCluster/data/10000358(3)-2015.07.31~2015.08.02.csv'
meta_detector.set_data(datafile)
#meta_detector.set_data('/home/nilm/analytics_master/NilmCluster/data/10000358(6)-2015.07.24~2015.07.31.csv')
#meta_detector.set_data('/home/nilm/analytics_master/NilmCluster/data/10000358(3)-2015.08.01~2015.08.02.csv')
print 'read complete...'
if( meta_detector.detect_apps() ):
	print meta_detector.get_app_info()

app_info = meta_detector.get_app_info(True)
#app_info_one = json.dumps(app_info['0'])

app_usage = AppDailyLoadR()
app_usage.set_data(datafile)
app_usage.set_app_info(app_info)
if( app_usage.do_compute() ):
	print 'usage (W): ' + str(app_usage.get_usage())
else:
	print 'faile to compute usage'
