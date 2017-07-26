#!/usr/bin/env python

# THEANO_FLAGS=device=gpu1,floatX=float32 python fit_model.py -i /disk1/raw_data_with_plug/dl-by-device/merged_2ch/jp-F3020073-tv_window-60-sec_v0.2.1.csv -o ../temp_learner -m 2ch_cnn_lstm -f encompact2chdiffaprp -p 600 -s 1000 -w 2ch_cnn_lstm_F3020073 -n 1 -t self
import sys
import optparse
from datetime import datetime
import numpy as np
import logging
import os.path
import time
import pandas as pd
import os
import logging


from enlearner import model, data, helper

def main():
    parser = optparse.OptionParser()
    define_options(parser)
    (options, args) = parser.parse_args()

    logger = helper.create_logger('eval_model')

    test_output = helper.next_filename(options.output+'/'+options.weights.split('/')[len(options.weights.split('/'))-1])
    os.mkdir(test_output)
    #test_output = options.output+'/'+options.weights.split('/')[len(options.weights.split('/'))-1]
    #os.mkdir(test_output)
    run_info_fp = open(test_output + '/evaluation.txt','a')

    run_info_fp.write('* Run prameters: {0}\n'.format(str(options)))

    start_ts = time.time()
    run_info_fp.write('* Start time: {0}\n'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())))

    data_model = data.DataManager(helper.file_path_to_list(options.input), options.format, logger, {'p_length':int(options.points)}).get_data()
    file_read_ts = time.time()
    run_info_fp.write('* Time for file reading: {0}\n'.format(str(file_read_ts-start_ts)))

    model_to_test = model.ModelManager(options.model,logger,{'num_column':int(options.points)}).get_model()
    model_to_test.fit(train_data=None, weights=options.weights+'.h5')

    run_info_fp.write('* Model Test (avg precision, precision, recall, f1)\n')
    f1_scores = []
    for one_test_id in data_model.data_ids:
        test_set = data_model.generate_test_set(opt={'test_site_ids': [one_test_id]})
        y = model_to_test.predict(test_set, opt={'verbose': 1})
        if options.start is not None and options.end is not None: 
            time_start = long(options.start)
            time_end = long(options.end)
        else:
            time_start = 0
            time_end = 2000000000
        import numpy as np
        #for t in np.linspace(0.95,1,20):
        #    output = model.evaluate(test_set, y, t, serial=one_test_id, output_dir=None, time_start=time_start,time_end=time_end)
        #    f1_scores.append(float(output.split(',')[4]))
        #    run_info_fp.write(' > {0} : {2} {1}'.format(one_test_id,str(output),str(t)) )
        #    run_info_fp.flush()
        output = model.evaluate(test_set, y, threshold=0.96, serial=one_test_id, output_dir=test_output, time_start=time_start,time_end=time_end)
        f1_scores.append(float(output.split(',')[4]))
        run_info_fp.write(' > {0} : {1}'.format(one_test_id,str(output)))
        run_info_fp.flush()
    import numpy as np
    run_info_fp.write(' >>> Median of F1 : {0}\n'.format(np.median(f1_scores))) 
    run_info_fp.write('* End time: {0}\n'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())))
    run_info_fp.close()

def define_options(parser):
    parser.add_option("-i", "--input", dest="input", help="Input for model training")
    parser.add_option("-o", "--output", dest="output", help="Directory for output") 
    parser.add_option("-m", "--model", dest="model", help="Model to test") 
    parser.add_option("-w", "--weights",dest="weights", help="Model weights")
    parser.add_option("-p", "--points", dest="points", help="Number of points for sampling")
    parser.add_option("-s", "--start", dest="start", help="Time to start")
    parser.add_option("-e", "--end", dest="end", help="Time to end")
    parser.add_option("-f", "--format", dest="format", help="Data format for processing")

if __name__ == "__main__":
    main()
