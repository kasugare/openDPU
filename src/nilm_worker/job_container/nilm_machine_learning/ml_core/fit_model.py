#!/usr/bin/env python

# THEANO_FLAGS=device=gpu1,floatX=float32 python fit_model.py -i /disk1/raw_data_with_plug/dl-by-device/merged_2ch/jp-F3020073-tv_window-60-sec_v0.2.1.csv -o ../temp_learner -m 2ch_cnn_lstm -f encompact2chdiffaprp -p 600 -s 1000 -w 2ch_cnn_lstm_F3020073 -n 1 -t self
# THEANO_FLAGS=device=gpu0,floatX=float32 python fit_model.py -i /disk1/raw_data_with_plug/dl-by-device-ac/merged_2ch/jp-F3020073-air-conditioner_window-60-sec_v0.2.1.csv -o  ../temp_learner -m 2ch_cnn_lstm -f encompact2chdiffaprp -p 600 -s 1500 -w ac_2ch_cnn_lstm_F3020073 -n 30 -t self
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

    # create logger
    logger = helper.create_logger('fit_model')

    filename_to_save = helper.next_filename(options.output+'/'+options.filename)
    run_info_fp = open(filename_to_save,'a')

    run_info_fp.write('* Run prameters: {0}\n'.format(str(options)))

    start_ts = time.time()
    run_info_fp.write('* Start time: {0}\n'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())))

    data_model = data.DataManager(helper.file_path_to_list(options.input), options.format, logger, {'p_length':int(options.points)}).get_data()
    train_set = data_model.generate_train_set(opt={'min_positive_sample': int(options.num_sample)})
    file_read_ts = time.time()
    run_info_fp.write('* Time for file reading: {0}\n'.format(str(file_read_ts-start_ts)))

    model_to_fit = model.ModelManager(options.model, logger, {'num_column': int(data_model.get_num_column())}).get_model()
    model_to_fit.fit(train_data=train_set, opt={'nb_epoch': int(options.nepoch)})
    model_fit_ts = time.time()
    run_info_fp.write('* Time for model fitting: {0}\n'.format(str(model_fit_ts-file_read_ts)))
    model_to_fit.save_weights(filename_to_save + '.h5')

    if options.test == 'self' :
        run_info_fp.write('* Model Test (avg precision, precision, recall, f1)\n')
        f1_scores = []
        for one_test_id in data_model.data_ids:
            test_set = data_model.generate_test_set(opt={'test_site_ids': [one_test_id]})
            y = model_to_fit.predict(input_data=test_set, opt={'verbose': 0})
            output = model.evaluate(test_set, y, threshold=0.80, serial=one_test_id)
            f1_scores.append(float(output.split(',')[4]))
            run_info_fp.write(' > {0} : {1}'.format(one_test_id,str(output)))
        import numpy as np
        run_info_fp.write(' >>> Median of F1 : {0}\n'.format(np.median(f1_scores))) 
    run_info_fp.write('* End time: {0}\n'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())))
    run_info_fp.close()

def define_options(parser):
    parser.add_option("-n", "--nepoch", dest="nepoch", help="Number of epoch")
    parser.add_option("-i", "--input", dest="input", help="Input for model training")
    parser.add_option("-o", "--output", dest="output", help="Directory for output") 
    parser.add_option("-m", "--model", dest="model", help="Model class name")
    parser.add_option("-f", "--format", dest="format", help="Data format for processing")
    parser.add_option("-s", "--num_sample", dest="num_sample", help="Number of sample for training")
    parser.add_option("-w", "--filename",dest="filename", help="Filename to save a model and weights")
    parser.add_option("-p", "--points", dest="points", help="Number of points for sampling")
    parser.add_option("-t", "--test", dest="test", help="Test files")


if __name__ == "__main__":
    main()
