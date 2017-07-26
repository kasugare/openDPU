	message = {
		"proto": "REQ_STUB_NILM_ML",
		"jobType": "NILM_ML",
		"params": {
			"sid": "10010592",
			"mlDataFilePath": "/Users/kasugare/Work/Encored/nilm_cluster/data/machine_learning_raw_data.csv",
			"modelFileName": "tv_2ch_cnn_lstm_12.h5",
			"country": "KR",
			"timezone": "Asia/Seoul",
			"modelName": "MODEL_NAME",
			"modelFormat": "DATA_FORMAT",
			"threshold": 0.231,
			"numOfPoint": 10
			}
		}

	parser.add_option("-i", "--input", dest="input", help="Input for model training")
    parser.add_option("-o", "--output", dest="output", help="Directory for output") 
    parser.add_option("-m", "--model", dest="model", help="Model to test") 
    parser.add_option("-w", "--weights",dest="weights", help="Model weights") <- fileName 
    parser.add_option("-p", "--points", dest="points", help="Number of points for sampling")
    parser.add_option("-f", "--format", dest="format", help="Data format for processing")

----------------------------------------
input 	= trainingDataFilePath : HDFS에서 받아올 ml용 rawdata file
output	= savedOutputDirPath
model 	= modelName
weights = modelWeight
points 	= numOfPoint
format  = dataFormat

- sid
- country
- timezone
- threshold
----------------------------------------

hadoop fs -rm -r /nilm/data/predict/JP_Tokyo/**/**/2016-12-0*


"sid": "10010592",
"mlDataFilePath": "/Users/kasugare/Work/Encored/nilm_cluster/data/machine_learning_raw_data.csv",
"modelFileName": "tv_2ch_cnn_lstm_12.h5",
"country": "KR",
"timezone": "Asia/Seoul",
"modelName": "MODEL_NAME",
"dataFormat": "DATA_FORMAT",
"threshold": 0.231,
"numOfPoint": 10


python StubClient.py  -s 10010592 -i /Users/kasugare/Work/Encored/nilm_cluster/data/machine_learning_raw_data.csv -o /Users/kasugare/Work/Encored/nilm_cluster/data/ -m tv_2ch_cnn_lstm_12.h5 -w cnn_1d_v20161217.h5 -p 10 -f /Users/kasugare/Work/Encored/nilm_cluster/data/machine_learning_raw_data.csv -c KR -z Asia/Seoul -t 1.0

python StubClient.py  -s 10010592 -i /Users/kasugare/Work/Encored/nilm_cluster/data/data_F3020064_20161125-20161126_00_F3020064.csv -o /Users/kasugare/Work/Encored/nilm_cluster/data/ -m cnn_1d -w cnn_1d_v20161217.h5 -p 10 -f encompact1chdiffstdfromraw -c KR -z Asia/Seoul -t 1.0
