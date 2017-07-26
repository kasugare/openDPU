# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import math
import logging
import random
import feather
from operator import add


def std_array(data):
    #print( 'mean: ' + str(np.mean(data)) + ', std:' + str(np.std(data)))
    return (data - np.mean(data)) / np.std(data)

def diff_array(data):
    #print 'diff array', data.shape
    output = data.diff(axis=1)
    output.iloc[:,0] = np.zeros(shape=(data.shape[0],1))
    #output = pd.concat([data.diff(axis=1),pd.DataFrame(np.zeros(shape=(data.shape[0],1)))],axis=1)
    #print output
    #print output.shape
    return output

def max_array(data):
    return (data - np.min(data)) / np.max(data)

def qmax_array(data):
    m = np.percentile(data,[0.1,99.9])

    return (data - m[0])/m[1] #np.minimum(np.maximum((data - m[0])/m[1],0.0),1.0)


class DataManager:
    def __init__(self, filename, format_name, logger, opt={}):
        self.opt = opt
        self.format_name = format_name
        if format_name == 'encompact':
            self.data = EnCompactForm(filename, logger, self.opt)
        elif format_name == 'encompactstd':
            self.data = EnCompactStd(filename, logger, self.opt)
        elif format_name == 'encompactdiff':
            self.data = EnCompactDiff(filename, logger, self.opt)
        elif format_name == 'encompactdiff01':
            self.data = EnCompactDiff01(filename, logger, self.opt)
        elif format_name == 'encompact2chdiff':
            self.data = EnCompact2ChDiff(filename, logger, self.opt)
        elif format_name == 'encompact2chdiff01': #EnCompact2ChApRp
            self.data = EnCompact2ChDiff01(filename, logger, self.opt)
        elif format_name == 'encompact2chdiffaprp': #
            self.data = EnCompact2ChApRp(filename, logger, self.opt)
        elif format_name == 'encompact2chdiffaprpfromraw': #
            self.data = EnCompact2ChApRpFromRaw(filename, logger, self.opt)
        elif format_name == 'encompactdiffstd':
            self.data = EnCompactDiffStd(filename, logger, self.opt)
        elif format_name == 'encompact01':
            self.data = EnCompact01(filename, logger, self.opt)
        elif format_name == 'encompact01p':
            self.data = EnCompact01P(filename, logger, self.opt)
        elif format_name == 'encompactdown':
            self.data = EnCompactDown(filename, logger, self.opt)
        elif format_name == 'encompact1chdiffstdfromraw':
            self.data = EnCompact1ChDiffStdFromRaw(filename, logger, self.opt)
        elif format_name == 'encompact1chstdfromraw':
            self.data = EnCompact1ChStdFromRaw(filename, logger, self.opt)
        else:
            raise Exception('Unsupported data format')


    def get_data(self):
        return self.data


class DataFormat:
    def __init__(self, filename, logger, opt):
        self.logger = logger
        self.opt = opt

        self.raw_data = None
        self.data_ids = None

        self.read_raw_data(filename)

        self.train_cached = {}

    def read_raw_data(self, filename):
        raise Exception('Not implemented')

    def preprocess(self, data, opt):
        pass

    def generate_train_set(self, opt):
        raise Exception('Not implemented')

    def generate_test_set(self, opt):
        raise Exception('Not implemented')

    def filter_raw_data(self, filename):
        pass

    def get_num_column(self):
        raise Exception('Not implemented')

    def get_num_input(self):
        raise Exception('Not implemented')



class EnCompactForm(DataFormat):
    def get_num_input(self):
        return 2

    def get_num_column(self):
        return self._idx_p_length

    def read_raw_data(self, filename):
        self._idx_serial = 0
        self._idx_timestamp = 1
        self._idx_p_length = int(self.opt['p_length'] if 'p_length' in self.opt else '900')
        self._idx_ap_begin = 2
        self._idx_rp_begin = self._idx_ap_begin + self._idx_p_length
        self._idx_onoff = self._idx_rp_begin + self._idx_p_length

        self.logger.debug('start file reading')
        self.raw_data = pd.DataFrame()
        filename = filename.split(',') if type(filename) is not list else filename
        for one_file in filename:
            if '.feather' in one_file:
                self.raw_data =  pd.concat([self.raw_data, feather.read_dataframe(one_file)], axis=0)
            else:
                self.raw_data = pd.concat([self.raw_data, pd.read_csv(one_file, header=0)], axis=0)

        self.logger.debug('end file reading')
        self.data_ids = np.unique(np.array(self.raw_data.loc[:, ['siteid']]))
        if 'id_to_exclude' in self.opt:
            self.logger.debug('id to exclude : ' + str(self.opt['id_to_exclude']))
            self.data_ids = np.setdiff1d(self.data_ids,self.opt['id_to_exclude'])
        self.logger.debug(self.data_ids)

    def preprocess(self, df, opt={}):
        return pd.concat([
                (df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)]),
                (df.iloc[:, self._idx_rp_begin:(self._idx_rp_begin + self._idx_p_length)])
            ], axis=1)

    def generate_train_set(self, opt={}):
        train_site_ids = opt['train_data_ids'] if 'train_data_ids' in opt else self.data_ids
        min_positive_sample = opt['min_positive_sample'] if 'min_positive_sample' in opt else 10 ** 10
        positive_negative_rate = opt['positive_negative_rate'] if 'positive_negative_rate' in opt else 1

        data_train_X = pd.DataFrame()
        data_train_Y = pd.DataFrame()

        for data_id in train_site_ids:

            self.logger.debug('Generating train set for : ' + str(data_id))
            if data_id not in self.train_cached:
                one_house_df = self.raw_data[self.raw_data.siteid == data_id]
                one_house_input = self.preprocess(one_house_df)

                print one_house_input.head()
                one_house_output = one_house_df.iloc[:, self._idx_onoff:(self._idx_onoff + 1)]
                self.logger.debug('onoff index' + str(self._idx_onoff))
                # one_house_output = one_house_df['onoff']

                one_house_input = np.array(one_house_input)
                one_house_output = np.array(one_house_output)

                one_house_on = one_house_input[one_house_output[:, 0] == 1]
                one_house_off = one_house_input[one_house_output[:, 0] == 0]

                num_positive = one_house_on.shape[0]
                num_positive_sample = min(num_positive, min_positive_sample,
                                          int(math.floor(one_house_off.shape[0] / positive_negative_rate)) )
                num_negative_sample = int(num_positive_sample * positive_negative_rate)

                if num_positive_sample == 0:
                    self.logger.warn('No data for this site: ' + str(data_id))
                    continue
                elif num_positive/float(one_house_input.shape[0]) > 0.75:
                    self.logger.warn('Too much on input for this site : ' + str(data_id))
                    continue
                elif num_positive_sample < min_positive_sample:
                    self.logger.warn(
                        'Not enough data for this site : ' + str(data_id) + ' ==> (num_pos, num_pos_from_neg, min_pos) '
                        + str([num_positive, int(math.floor(one_house_off.shape[0] / positive_negative_rate)),min_positive_sample]))
                    #self.logger.info('Skip this site')
                    #continue

                sample_on = pd.DataFrame(one_house_on).sample(n=num_positive_sample)
                sample_off = pd.DataFrame(one_house_off).sample(n=num_negative_sample)

                self.train_cached[data_id] = dict()
                self.train_cached[data_id]['on'] = sample_on
                self.train_cached[data_id]['off'] = sample_off

            #
            # from cache
            sample_on = self.train_cached[data_id]['on']
            sample_off = self.train_cached[data_id]['off']
            num_positive_sample = sample_on.shape[0]
            num_negative_sample = sample_off.shape[0]
            #

            data_train_X = data_train_X.append(sample_on)
            data_train_Y = data_train_Y.append(
                pd.DataFrame((np.array([1] * num_positive_sample)).reshape(num_positive_sample, 1)))

            data_train_X = data_train_X.append(sample_off)
            data_train_Y = data_train_Y.append(
                pd.DataFrame((np.array([0] * num_negative_sample)).reshape(num_negative_sample, 1)))

            #self.logger.debug(sample_on.head())
            #self.logger.debug(sample_off.head())

        return {'X': np.array(data_train_X), 'Y': np.array(data_train_Y)}

    def generate_test_set(self, opt={}):
        test_site_ids = opt['test_site_ids'] if 'test_site_ids' in opt else [self.data_ids[random.randrange(0, len(self.data_ids))]]

        data_test_X = pd.DataFrame()
        data_test_Y = pd.DataFrame()
        data_test_timestamp = pd.DataFrame()

        for data_id in test_site_ids:
            one_house_df = self.raw_data[self.raw_data.siteid == data_id]
            #print data_id, 'one_house_df', one_house_df.shape
            one_house_input = self.preprocess(one_house_df)
            #print data_id, 'process', pd.DataFrame(one_house_input).shape

            one_house_output = one_house_df.iloc[:, self._idx_onoff:(self._idx_onoff + 1)]
            one_house_timestamp = one_house_df.iloc[:, self._idx_timestamp:(self._idx_timestamp + 1)]

            #print one_house_df.shape, one_house_input.shapetest_site_id
            data_test_X = data_test_X.append(one_house_input)
            data_test_Y = data_test_Y.append(one_house_output)
            data_test_timestamp = data_test_timestamp.append(one_house_timestamp)

            #print data_id, data_test_X.shape, data_test_Y.shape

        return {'X': np.array(data_test_X), 'Y': np.array(data_test_Y), 'ts': np.array(data_test_timestamp)}

class EnCompact01(EnCompactForm):
    def preprocess(self, df, opt={}):
        return pd.concat([
                max_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)]),
                max_array(df.iloc[:, self._idx_rp_begin:(self._idx_rp_begin + self._idx_p_length)])
            ], axis=1)


class EnCompactStd(EnCompactForm):
    def preprocess(self, df, opt={}):
        return pd.concat([
            std_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)]),
            std_array(df.iloc[:, self._idx_rp_begin:(self._idx_rp_begin + self._idx_p_length)])
        ], axis=1)


class EnCompactDiff(EnCompactForm):
    def preprocess(self, df, opt={}):
        return pd.concat([
                diff_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)]),
                diff_array(df.iloc[:, self._idx_rp_begin:(self._idx_rp_begin + self._idx_p_length)])
            ], axis=1)

class EnCompactDiff01(EnCompactForm):
    def preprocess(self, df, opt={}):
        return pd.concat([
                diff_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)]),
                max_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)])
            ], axis=1)

class EnCompact2ChDiff(EnCompactForm):
    def get_num_input(self):
        return 2

    def get_num_column(self):
        return self._idx_p_length

    def read_raw_data(self, filename):
        self._idx_serial = 0
        self._idx_timestamp = 1
        self._idx_p_length = int(self.opt['p_length'] if 'p_length' in self.opt else '900')
        self._idx_ap_begin = 2
        self._idx_rp_begin = self._idx_ap_begin + self._idx_p_length
        self._idx_onoff = self._idx_ap_begin + 4*self._idx_p_length

        self.logger.debug('start file reading')
        self.raw_data = pd.DataFrame()
        filename = filename.split(',') if type(filename) is not list else filename
        for one_file in filename:
            if '.feather' in one_file:
                self.raw_data =  pd.concat([self.raw_data, feather.read_dataframe(one_file)], axis=0)
            else:
                self.raw_data = pd.concat([self.raw_data, pd.read_csv(one_file, header=0)], axis=0)

        self.logger.debug('end file reading')
        self.data_ids = np.unique(np.array(self.raw_data.loc[:, ['siteid']]))
        if 'id_to_exclude' in self.opt:
            self.logger.debug('id to exclude : ' + str(self.opt['id_to_exclude']))
            self.data_ids = np.setdiff1d(self.data_ids,self.opt['id_to_exclude'])
        self.logger.debug(self.data_ids)

    def preprocess(self, df, opt={}):
        return pd.concat([
                diff_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)]),
                #max_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)]),
                diff_array(df.iloc[:, (self._idx_ap_begin + 2*self._idx_p_length):(self._idx_ap_begin + 3*self._idx_p_length)])
                #max_array(df.iloc[:, (self._idx_ap_begin + 2*self._idx_p_length):(self._idx_ap_begin + 3*self._idx_p_length)])
            ], axis=1)

class EnCompact2ChDiff01(EnCompactForm):
    def get_num_input(self):
        return 4

    def get_num_column(self):
        return self._idx_p_length

    def read_raw_data(self, filename):
        self._idx_serial = 0
        self._idx_timestamp = 1
        self._idx_p_length = int(self.opt['p_length'] if 'p_length' in self.opt else '900')
        self._idx_ap_begin = 2
        self._idx_rp_begin = self._idx_ap_begin + self._idx_p_length
        self._idx_onoff = self._idx_ap_begin + 4*self._idx_p_length

        self.logger.debug('start file reading')
        self.raw_data = pd.DataFrame()
        print filename
        for one_file in filename.split(','):
            if '.feather' in one_file:
                self.raw_data =  pd.concat([self.raw_data, feather.read_dataframe(one_file)], axis=0)
            else:
                self.raw_data = pd.concat([self.raw_data, pd.read_csv(one_file, header=0)], axis=0)
            print one_file
            print self.raw_data.tail()

        self.logger.debug('end file reading')
        self.data_ids = np.unique(np.array(self.raw_data.loc[:, ['siteid']]))
        if 'id_to_exclude' in self.opt:
            self.logger.debug('id to exclude : ' + str(self.opt['id_to_exclude']))
            self.data_ids = np.setdiff1d(self.data_ids,self.opt['id_to_exclude'])
        self.logger.debug(self.data_ids)

    def preprocess(self, df, opt={}):
        return pd.concat([
                diff_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)]),
                max_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)]),
                diff_array(df.iloc[:, (self._idx_ap_begin + 2*self._idx_p_length):(self._idx_ap_begin + 3*self._idx_p_length)]),
                max_array(df.iloc[:, (self._idx_ap_begin + 2*self._idx_p_length):(self._idx_ap_begin + 3*self._idx_p_length)])
            ], axis=1)


class EnCompact2ChApRp(EnCompactForm):
    def get_num_input(self):
        return 4

    def get_num_column(self):
        return self._idx_p_length

    def read_raw_data(self, filename):
        self._idx_serial = 0
        self._idx_timestamp = 1
        self._idx_p_length = int(self.opt['p_length'] if 'p_length' in self.opt else '900')
        self._idx_ap_begin = 2
        self._idx_rp_begin = self._idx_ap_begin + self._idx_p_length
        self._idx_onoff = self._idx_ap_begin + 4*self._idx_p_length

        self.logger.debug('start file reading')
        self.raw_data = pd.DataFrame()
        filename = filename.split(',') if type(filename) is not list else filename
        for one_file in filename:
            if '.feather' in one_file:
                self.raw_data =  pd.concat([self.raw_data, feather.read_dataframe(one_file)], axis=0)
            else:
                print( pd.read_csv(one_file, header=0).tail() )
                self.raw_data = pd.concat([self.raw_data, pd.read_csv(one_file, header=0)], axis=0)

        self.logger.debug('end file reading')
        self.data_ids = np.unique(np.array(self.raw_data.loc[:, ['siteid']]))
        if 'id_to_exclude' in self.opt:
            self.logger.debug('id to exclude : ' + str(self.opt['id_to_exclude']))
            self.data_ids = np.setdiff1d(self.data_ids,self.opt['id_to_exclude'])
        self.logger.debug(self.data_ids)

    def preprocess(self, df, opt={}):
        return pd.concat([
                diff_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)]),
                diff_array(df.iloc[:, (self._idx_ap_begin + 1*self._idx_p_length):(self._idx_ap_begin + 2*self._idx_p_length)]),
                diff_array(df.iloc[:, (self._idx_ap_begin + 2*self._idx_p_length):(self._idx_ap_begin + 3*self._idx_p_length)]),
                diff_array(df.iloc[:, (self._idx_ap_begin + 3*self._idx_p_length):(self._idx_ap_begin + 4*self._idx_p_length)])
            ], axis=1)

class EnCompact2ChApRpFromRaw(EnCompact2ChApRp):
    def _make_data_frame(self,filename):
        only_name = filename.split('/')[len(filename.split('/'))-1]
        serial = only_name.split('_')[1]
        temp_buffer_ap1 = []
        tansformed_row = []

        with open(filename) as fp:
            count_total = 0
            for oneline in fp:
                arr = oneline.split(',')
                count_sec = count_total % 60
                if count_sec == 0:
                    if temp_buffer_ap1 is not None and len(temp_buffer_ap1) == 600 :
                        tansformed_row.append( [serial, float(timestamp)] + temp_buffer_ap1 + temp_buffer_rp1 + temp_buffer_ap2
                        + temp_buffer_rp2 + [1] )

                    timestamp = arr[0]
                    temp_buffer_ap1 = []
                    temp_buffer_rp1 = []
                    temp_buffer_ap2 = []
                    temp_buffer_rp2 = []
                temp_buffer_ap1 = temp_buffer_ap1 + map(lambda x: float(x), arr[3:13])
                temp_buffer_rp1 = temp_buffer_rp1 + map(lambda x: float(x), arr[13:23])
                temp_buffer_ap2 = temp_buffer_ap2 + map(lambda x: float(x), arr[25:35])
                temp_buffer_rp2 = temp_buffer_rp2 + map(lambda x: float(x), arr[35:45])
                
                count_total = count_total + 1

            transformed_pd = pd.DataFrame(tansformed_row)
            transformed_pd.columns = ['siteid','timestamp'] + [str(i) for i in range(0,2400)] + ['onoff']

            return transformed_pd

            
    def read_raw_data(self, filename):
        self._idx_serial = 0
        self._idx_timestamp = 1
        self._idx_p_length = int(self.opt['p_length'] if 'p_length' in self.opt else '900')
        self._idx_ap_begin = 2
        self._idx_rp_begin = self._idx_ap_begin + self._idx_p_length
        self._idx_onoff = self._idx_ap_begin + 4*self._idx_p_length

        self.logger.debug('start file reading')
        self.raw_data = pd.DataFrame()
        filename = filename.split(',') if type(filename) is not list else filename
        for one_file in filename:
            self.raw_data =  pd.concat([self.raw_data, self._make_data_frame(one_file)], axis=0)

        self.logger.debug('end file reading')
        self.data_ids = np.unique(np.array(self.raw_data.loc[:, ['siteid']]))
        if 'id_to_exclude' in self.opt:
            self.logger.debug('id to exclude : ' + str(self.opt['id_to_exclude']))
            self.data_ids = np.setdiff1d(self.data_ids,self.opt['id_to_exclude'])
        self.logger.debug(self.data_ids)

    def preprocess(self, df, opt={}):
        return pd.concat([
                diff_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)]),
                diff_array(df.iloc[:, (self._idx_ap_begin + 1*self._idx_p_length):(self._idx_ap_begin + 2*self._idx_p_length)]),
                diff_array(df.iloc[:, (self._idx_ap_begin + 2*self._idx_p_length):(self._idx_ap_begin + 3*self._idx_p_length)]),
                diff_array(df.iloc[:, (self._idx_ap_begin + 3*self._idx_p_length):(self._idx_ap_begin + 4*self._idx_p_length)])
            ], axis=1)


class EnCompact1ChDiffStdFromRaw(EnCompact2ChApRp):
    def _make_data_frame(self,filename):
        try:
            only_name = filename.split('/')[len(filename.split('/'))-1]
            serial = only_name.split('_')[1]
            if int(serial, 16) > 0 :
                serial = serial
        except:
            serial = '99999999'

        temp_buffer_ap1 = []
        tansformed_row = []

        with open(filename) as fp:
            count_total = 0
            for oneline in fp:
                arr = oneline.split(',')
                count_sec = count_total % 60
                if count_sec == 0:
                    if temp_buffer_ap1 is not None and len(temp_buffer_ap1) == 600 :
                        tansformed_row.append( [serial, float(timestamp)] + 
                            map(add, temp_buffer_ap1,temp_buffer_ap2) +
                            map(add, temp_buffer_rp1,temp_buffer_rp2) + [1] )
                        #self.logger.debug('time elasped: ' + str(float(arr[0])-float(timestamp)))

                    timestamp = arr[0]
                    temp_buffer_ap1 = []
                    temp_buffer_rp1 = []
                    temp_buffer_ap2 = []
                    temp_buffer_rp2 = []
                temp_buffer_ap1 = temp_buffer_ap1 + map(lambda x: float(x), arr[3:13])
                temp_buffer_rp1 = temp_buffer_rp1 + map(lambda x: float(x), arr[13:23])
                temp_buffer_ap2 = temp_buffer_ap2 + map(lambda x: float(x), arr[25:35])
                temp_buffer_rp2 = temp_buffer_rp2 + map(lambda x: float(x), arr[35:45])
                
                count_total = count_total + 1

            transformed_pd = pd.DataFrame(tansformed_row)
            transformed_pd.columns = ['siteid','timestamp'] + [str(i) for i in range(0,1200)] + ['onoff']
            #transformed_pd.to_csv('/tmp/xxxxxxx.csv')
            #self.logger.debug(transformed_pd)

            return transformed_pd

    def read_raw_data(self, filename):
        self._idx_serial = 0
        self._idx_timestamp = 1
        self._idx_p_length = int(self.opt['p_length'] if 'p_length' in self.opt else '900')
        self._idx_ap_begin = 2
        self._idx_rp_begin = self._idx_ap_begin + self._idx_p_length
        self._idx_onoff = self._idx_ap_begin + 2*self._idx_p_length

        self.logger.debug('start file reading')
        self.raw_data = pd.DataFrame()
        filename = filename.split(',') if type(filename) is not list else filename
        for one_file in filename:
            self.raw_data =  pd.concat([self.raw_data, self._make_data_frame(one_file)], axis=0)

        self.logger.debug('end file reading')
        self.data_ids = np.unique(np.array(self.raw_data.loc[:, ['siteid']]))
        if 'id_to_exclude' in self.opt:
            self.logger.debug('id to exclude : ' + str(self.opt['id_to_exclude']))
            self.data_ids = np.setdiff1d(self.data_ids,self.opt['id_to_exclude'])
        self.logger.debug(self.data_ids)

    def preprocess(self, df, opt={}):
        if 'stat_info' in self.opt :
            stat_info = self.opt['stat_info']
            stat_info = stat_info.split(',')
            ap_mean = float(stat_info[0].split(':')[1])
            ap_std = float(stat_info[1].split(':')[1])
            rp_mean = float(stat_info[2].split(':')[1])
            rp_std = float(stat_info[3].split(':')[1])
            self.logger.debug('stat info: ' + str(self.opt['stat_info']))
        else:
            ap_mean = np.mean(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)])
            ap_std = np.std(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)])
            rp_mean = np.mean(df.iloc[:, self._idx_rp_begin:(self._idx_rp_begin + self._idx_p_length)])
            rp_std = np.std(df.iloc[:, self._idx_rp_begin:(self._idx_rp_begin + self._idx_p_length)])

        #ap_mean = 298.377371
        #ap_std = 286.380454

        diff_arr = df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)].diff(axis=1)
        diff_arr.iloc[:,0] = np.zeros(shape=(diff_arr.shape[0],1))

        output = pd.concat([
                diff_arr,
                ((df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)])-ap_mean)/ap_std
            ], axis=1)
        #self.logger.debug(output.head())
        return output

class EnCompact1ChStdFromRaw(EnCompact1ChDiffStdFromRaw):
    def preprocess(self, df, opt={}):
        if ('ap_mean' not in opt) or ('rp_mean' not in opt):
            ap_mean = np.mean(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)])
            ap_std = np.std(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)])
            rp_mean = np.mean(df.iloc[:, self._idx_rp_begin:(self._idx_rp_begin + self._idx_p_length)])
            rp_std = np.std(df.iloc[:, self._idx_rp_begin:(self._idx_rp_begin + self._idx_p_length)])
        else:
            ap_mean = float(opt['ap_mean'])
            ap_std = float(opt['ap_std'])
            rp_mean = float(opt['rp_mean'])
            rp_std = float(opt['rp_std'])

        return pd.concat([
                ((df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)])-ap_mean)/ap_std,
                ((df.iloc[:, self._idx_rp_begin:(self._idx_rp_begin + self._idx_p_length)])-rp_mean)/rp_std
            ], axis=1)


class EnCompactDiffStd(EnCompactForm):
    def preprocess(self, df, opt={}):
        return pd.concat([
                diff_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)]),
                std_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)])
            ], axis=1)


class EnCompact01P(EnCompactForm):
    def preprocess(self, df, opt={}):
        return pd.concat([
                qmax_array(df.iloc[:, self._idx_ap_begin:(self._idx_ap_begin + self._idx_p_length)]),
                qmax_array(df.iloc[:, self._idx_rp_begin:(self._idx_rp_begin + self._idx_p_length)])
            ], axis=1)


def down_sample(idata,nsample_from, nsample_to):
    new_idx = 0
    step = int(nsample_from/nsample_to)
    new_data = pd.DataFrame(0, index=np.arange(len(idata)),
                            columns=['siteid','timestamp']+map(lambda x: str(x),range(0,nsample_to*2))+['air-conditioner'])
    print idata.head()
    data = idata.reset_index()
    new_data['siteid'] = data['siteid']
    new_data['timestamp'] = data['timestamp']
    new_data['air-conditioner'] = data['air-conditioner']
    #new_data.iloc[:,'timestamp'] = data.iloc[:,'timestamp']
    #new_data.iloc[:,'air-conditioner'] = data.iloc[:,'air-conditioner']
    #new_data.iloc[:,(len(new_data.columns)-1):len(new_data.columns)] = data.iloc[:,(len(data.columns)-1):len(data.columns)]
    print new_data.head()
    for i in range(0,2*nsample_from,step):
        if (i+step)>len(data.columns):
            continue
        new_data.iloc[:,(2+new_idx)] = data.iloc[:,(2+i):(2+i+step)].mean(axis=1)
        new_idx += 1

    print new_data.head()
    return new_data

class EnCompactDown(EnCompact01):
    def read_raw_data(self, filename):
        EnCompactForm.read_raw_data(self,filename)
        self.raw_data = down_sample(self.raw_data,3000,300)
