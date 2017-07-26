#!/usr/bin/env python

#KERAS_BACKEND=theano ./run_model.py -i ./data/merged.csv -d ./temp -n 1 -x output_sample -m cnn_1d -f encompact -p 600
# KERAS_BACKEND=theano ./run_model.py -i ./data/merged_08_tv.csv.gz -d ./temp -n 1 -x output_sample_local -m cnn_1d_1min_110 -f encompact_positive -p 600 -s 3000
# KERAS_BACKEND=theano ./run_model.py -i ./data/merged_08_tv.csv.gz -d ./temp -n 1 -x output_sample_local -m lstm_simple -f encompact -p 600 -s 3000
# -e F3FDFFD5,F3FDFFBD,F3FDFFD5,F3FDFFF0,F3FDFFE4,F3FDFFE0,F3FDFFDE,F3FDFFF4,F3FDFFBC,F3FDFFD2,F3FDFFF1
import sys
import optparse
from datetime import datetime
import numpy as np
import logging
import os.path
import time
import pandas as pd
import os


from enlearner import model, data

parser = optparse.OptionParser()
parser.add_option("-n", "--nepoch", dest="nepoch",
                  help="Number of epoch")
parser.add_option("-i", "--input", dest="input",
                  help="Input filename")
parser.add_option("-d", "--dir", dest="dir",
                  help="Output directory")
parser.add_option("-x", "--prefix", dest="prefix",
                  help="Output filename prefix")
parser.add_option("-m", "--model", dest="model",
                  help="Model class name")
parser.add_option("-f", "--format", dest="format",
                  help="Data format class name")
parser.add_option("-p", "--points", dest="points",
                  help="Number of points for sampling")
parser.add_option("-s", "--num_sample", dest="num_sample",
                  help="Number of sample for training")
parser.add_option("-e", "--id_to_exclude", dest="id_to_exclude",
                  help="IDs to exclude")
parser.add_option("-k", "--kfold", dest="kfold",
                  help="f-fold CV")
parser.add_option("-z", "--selftest", dest="selftest",
                  help="Self test?")
parser.add_option("-t", "--testids", dest="testids",
                  help="Test ids")
parser.add_option("-w", "--write_file", dest="write_file",
                  help="Write model and weights to a file")

(options, args) = parser.parse_args()

# create logger
logger = logging.getLogger('pyenleaner')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.debug('start')

if options.id_to_exclude is not None:
    id_to_exclude = options.id_to_exclude.split(',')
else:
    id_to_exclude = []
logger.debug( 'id to exclude: ' + str(id_to_exclude))


if os.path.isdir(options.input):
    input_files = [ f for f in os.listdir(options.input) if os.path.isfile(os.path.join(options.input, options.input + '/' + f))]
    input_files = ','.join(map(lambda x: options.input + '/' + x,input_files))
    print input_files
else:
    input_files = options.input

print 'input files',input_files
my_data = data.DataManager(input_files, options.format, logger,
                           {'p_length':int(options.points),'id_to_exclude': id_to_exclude }).get_data()

if options.testids is None:
    if options.kfold is not None:
        nsample = int(len(my_data.data_ids)/float(options.kfold))
        kfold = int(options.kfold)
    else:
        nsample = 1
        kfold = len(my_data.data_ids)

    data_ids_temp = my_data.data_ids.tolist()
    test_ids = []
    for k in range(0,kfold):
        test_id = pd.Series(data_ids_temp).sample(nsample).tolist()
        data_ids_temp = np.setdiff1d(data_ids_temp,test_id)
        test_ids.append(test_id)
else:
    test_ids = []
    test_ids.append(options.testids.split(','))

print 'test id sets:' + str(test_ids)

f = open(options.dir + '/' + options.prefix + '_output.txt', 'a')
f.write(str(datetime.now()) + '\n')
f.write(str(options) + '\n')
f.write('avg_precision, precision, recall, f1 \n')
f.flush()
#f.close()

if options.write_file is not None:
    if os.path.exists(options.write_file):
        model_weight_to_write = options.write_file + '_' + str(time.time())
    else:
        model_weight_to_write = options.write_file
else:
    model_weight_to_write = None

my_model = model.ModelManager(options.model, logger, {'num_column': int(options.points)}).get_model()
if model_weight_to_write is not None:
    my_model.save_mode(model_weight_to_write)

#test_ids = my_data.data_ids.tolist()
for test_id in test_ids:
    print str(test_id)

    if options.selftest is None:
        train_set_ids = np.setdiff1d(my_data.data_ids, test_id)
    else:
        train_set_ids = my_data.data_ids

    weight_filename = options.dir + '/' + options.prefix + '_' + str(test_id) + '_weight.h5'
    logger.debug('train:' + str(train_set_ids) )

    if os.path.isfile(weight_filename):
        my_model.fit(weight=weight_filename)
    else:
        train_set = my_data.generate_train_set(opt={'min_positive_sample': int(options.num_sample),
                                                    'train_data_ids': train_set_ids,
                                                    'positive_negative_rate':1.0})
        if len(train_set['X']) == 0:
            print 'No Data!'
            continue
        my_model.fit(train_data=train_set, opt={'nb_epoch': int(options.nepoch)})
        my_model.save_weights(weight_filename)

    for single_id in test_id:
        print 'test:', str(single_id)
        test_set = my_data.generate_test_set(opt={'test_site_ids': [single_id]})
        y = my_model.predict(input_data=test_set, opt={'verbose': 0})
        output = model.evaluate(test_set,
                                y,
                                threshold=0.65,
                                serial=single_id,
                                output_dir=options.dir)

        #
        X_measured = map(lambda x: 1 if float(x) > 0.0 else 0, np.array(test_set['Y']))
        X_predicted = map(lambda x: 1 if x[1] > 0.65 else 0, np.array(y))
        print 'number of on', np.sum(X_measured), np.sum(X_predicted)

        print output
        f.write(output)
        f.flush()
    #f.write('{0:s}, {1:.3f}, {2:.3f}, {3:.3f}, {4:.3f}\n'.format(test_id, output[0], output[1], output[2], output[3]))
    #f.flush()

    #outputFile = 'yazaki_testset_ac_output.csv'
    #my_model.writePredict(test_site_id, test_set, outputFile)

f.close()
