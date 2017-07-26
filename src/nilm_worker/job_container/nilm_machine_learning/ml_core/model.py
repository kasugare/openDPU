# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import math

from keras.models import Graph, Sequential, model_from_json
from keras.models import *
from keras.layers.core import Dense, Activation, Flatten, Reshape
from keras.layers.recurrent import LSTM
from keras.optimizers import Adam
from keras.utils import np_utils
from keras.utils.np_utils import accuracy
from keras.optimizers import SGD
from keras.layers.normalization import BatchNormalization
from keras.layers import Dense, Dropout, Activation
from keras.layers.convolutional import Convolution1D, MaxPooling1D
from sklearn import metrics

import h5py
import logging


class ModelManager:
    def __init__(self, model_name, logger, opt={}):
        self.modelName = model_name
        if model_name == 'cnn_1d':
            self.model = CNN1DModel(logger,opt)
        elif model_name == 'cnn_1d_simple':
            self.model = CNN1DSimple(logger,opt)
        elif model_name == 'cnn_1d_1min_110':
            self.model = CNN1D1Min_1_1_0(logger,opt)
        elif model_name == 'cnn_1d_5min_410':
            self.model = CNN1D5Min_4_1_0(logger, opt)
        elif model_name == 'cnn_lstm':
            self.model = CNN1DLSTM(logger, opt)
        elif model_name == '2ch_cnn_lstm': #
            self.model = CNN1DLSTM2Ch(logger, opt)
        elif model_name == '2ch_lstm': #
            self.model = LSTMModel2Ch(logger, opt)
        elif model_name == '2ch_cnn_lstm_simple':  #
            self.model = CNN1DLSTM2ChSimple(logger, opt)
        elif model_name == 'lstm':
            self.model = LSTMModel(logger, opt)
        elif model_name == 'lstm_simple':
            self.model = LSTMSimple(logger, opt)
        else:
            raise Exception('Unsupported model')

    def get_model(self):
        return self.model


#
# BaseModel
#
# + create      : model creation
# + fit         : model fitting
# + predict     : prediction value
#
# (Optional)
# + postprocess :
#
class BaseModel:
    def __init__(self, logger,opt):
        self.logger = logger
        self.opt = opt
        self.model = None
        self.create()

    def create(self):
        raise Exception('Not implemented')

    def fit(self, train_data=None, weights=None, opt={}):
        raise Exception('Not implemented')

    def predict(self, input_data, opt={}):
        raise Exception('Not implemented')

    def postprocess(self, opt={}):
        pass

    def save_weights(self, filename):
        self.model.save_weights(filename, overwrite=True)



#
# CNN1MinModel
#
#
class CNN1DModel(BaseModel):
    def create(self):
        num_column = int(self.opt['num_column'])

        self.model = Graph()
        self.model.add_input(name='input1', input_shape=(num_column, 1))
        self.model.add_input(name='input2', input_shape=(num_column, 1))

        self.model.add_node(Convolution1D(32, 15, border_mode='same', input_shape=(num_column, 1)), name='conv1',input='input1')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool1', input='conv1')
        self.model.add_node(Convolution1D(128, 5, border_mode='same', input_shape=(num_column, 1)), name='conv2',input='Mpool1')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool2', input='conv2')
        self.model.add_node(Convolution1D(256, 3, border_mode='same', input_shape=(num_column, 1)), name='conv3',input='Mpool2')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool3', input='conv3')
        self.model.add_node(Convolution1D(512, 3, border_mode='same', input_shape=(num_column, 1)), name='conv4',input='Mpool3')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool4', input='conv4')

        self.model.add_node(Convolution1D(32, 15, border_mode='same', input_shape=(num_column, 1)), name='conv11',input='input2')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool11', input='conv11')
        self.model.add_node(Convolution1D(128, 5, border_mode='same', input_shape=(num_column, 1)), name='conv22',input='Mpool11')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool22', input='conv22')
        self.model.add_node(Convolution1D(256, 3, border_mode='same', input_shape=(num_column, 1)), name='conv33',input='Mpool22')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool33', input='conv33')
        self.model.add_node(Convolution1D(512, 3, border_mode='same', input_shape=(num_column, 1)), name='conv44',input='Mpool33')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool44', input='conv44')

        self.model.add_node(LSTM(128), name='forward1', inputs=['Mpool4', 'Mpool44'])
        self.model.add_node(LSTM(128, go_backwards=True), name='backward1', inputs=['Mpool4', 'Mpool44'])
        self.model.add_node(Dropout(0.5), name='dropout1', inputs=['forward1', 'backward1'])
        self.model.add_node(Dense(2, activation='softmax'), name='softmax', input='dropout1')

        self.model.add_output(name='output', input='softmax')
        #self.model = make_parallel(self.model, 2)

        #self.model.compile('adam', {'output': 'categorical_crossentropy'})
        adam = Adam(lr=0.000005, beta_1=0.9, beta_2=0.999, epsilon=1e-08)
        self.model.compile(adam, {'output': 'mean_absolute_error'})

        self.logger.debug('model is successfully created.')

    def fit(self, train_data=None, weights=None, opt={}):
        num_column = int(self.opt['num_column'])

        if weights is not None:
            self.model.load_weights(weights)
        else:
            batch_size = 256 if 'batch_size' not in opt else int(opt['batch_size'])
            nb_epoch = 1 if 'nb_epoch' not in opt else int(opt['nb_epoch'])
            show_accuracy = True if 'show_accuracy' not in opt else bool(opt['show_accuracy'])
            verbose = 1 if 'verbose' not in opt else int(opt['verbose'])

            self.model.fit({
                'input1': (np.array(train_data['X'])[:, 0:num_column]).reshape(train_data['X'].shape[0], num_column, 1),
                'input2': (np.array(train_data['X'])[:, num_column:(num_column*2)]).reshape(train_data['X'].shape[0], num_column, 1),
                'output': np_utils.to_categorical(np.array(train_data['Y']), 2)},
                batch_size=batch_size, nb_epoch=nb_epoch, show_accuracy=show_accuracy, verbose=verbose)

    def predict(self, input_data, opt={}):
        num_column = int(self.opt['num_column'])
        batch_size = 128 if 'batch_size' not in opt else int(opt['batch_size'])
        verbose = 1 if 'verbose' not in opt else int(opt['verbose'])

        predicted = self.model.predict({
            'input1': (np.array(input_data['X'])[:, 0:num_column]).reshape(input_data['X'].shape[0], num_column, 1),
            'input2': (np.array(input_data['X'])[:, num_column:(num_column*2)]).reshape(input_data['X'].shape[0], num_column, 1)},
            verbose=verbose, batch_size=batch_size)
        #print predicted
        predicted = predicted['output'].tolist()

        return predicted

class LSTMModel(CNN1DModel):
    def create(self):
        num_column = int(self.opt['num_column'])

        self.model = Graph()
        self.model.add_input(name='input1', input_shape=(num_column, 1))
        self.model.add_input(name='input2', input_shape=(num_column, 1))
        self.model.add_node(LSTM(32), name='forward1', inputs=['input1', 'input2'])
        self.model.add_node(LSTM(32, go_backwards=True), name='backward1', inputs=['input1', 'input2'])
        self.model.add_node(Dropout(0.5), name='dropout1', inputs=['forward1', 'backward1'])
        self.model.add_node(Dense(2, activation='softmax'), name='softmax', input='dropout1')
        self.model.add_output(name='output', input='softmax')
        self.model.compile('adam', {'output': 'categorical_crossentropy'})

        self.logger.debug('model is successfully created.')

class LSTMSimple(CNN1DModel):
    def create(self):
        num_column = int(self.opt['num_column'])

        self.model = Graph()
        self.model.add_input(name='input1', input_shape=(num_column, 1))
        self.model.add_input(name='input2', input_shape=(num_column, 1))
        self.model.add_node(LSTM(5), name='forward1', inputs=['input1','input2'])
        self.model.add_node(LSTM(5, go_backwards=True), name='backward1', inputs=['input1','input2'])
        self.model.add_node(Dropout(0.5), name='dropout1', inputs=['forward1', 'backward1'])
        self.model.add_node(Dense(2, activation='softmax'), name='softmax', input='dropout1')
        self.model.add_output(name='output', input='softmax')
        self.model.compile('adam', {'output': 'categorical_crossentropy'})

        self.logger.debug('model is successfully created.')

class CNN1D5Min_4_1_0(CNN1DModel):
    def create(self):
        num_column = int(self.opt['num_column'])
        print num_column

        self.model = Graph()

        self.model.add_input(name='input1', input_shape=(num_column, 1))
        self.model.add_input(name='input2', input_shape=(num_column, 1))

        self.model.add_node(Convolution1D(1, 15, border_mode='same'), name='conv1', inputs=['input1', 'input2'])
        self.model.add_node(BatchNormalization(mode=2), name='batch_norm1', input='conv1')
        self.model.add_node(Activation('relu'), name='relu1', input='batch_norm1')
        self.model.add_node(MaxPooling1D(pool_length=3, border_mode='valid'), name='Mpool1', input='relu1')

        self.model.add_node(Convolution1D(2, 15, border_mode='same'), name='conv2', input='Mpool1')
        self.model.add_node(BatchNormalization(mode=2), name='batch_norm2', input='conv2')
        self.model.add_node(Activation('relu'), name='relu2', input='batch_norm2')
        self.model.add_node(MaxPooling1D(pool_length=3, border_mode='valid'), name='Mpool2', input='relu2')

        self.model.add_node(Convolution1D(4, 15, border_mode='same'), name='conv3', input='Mpool2')
        self.model.add_node(BatchNormalization(mode=2), name='batch_norm3', input='conv3')
        self.model.add_node(Activation('relu'), name='relu3', input='batch_norm3')
        self.model.add_node(MaxPooling1D(pool_length=3, border_mode='valid'), name='Mpool3', input='relu3')

        self.model.add_node(Convolution1D(8, 15, border_mode='same'), name='conv4', input='Mpool3')
        self.model.add_node(BatchNormalization(mode=2), name='batch_norm4', input='conv4')
        self.model.add_node(Activation('relu'), name='relu4', input='batch_norm4')
        self.model.add_node(MaxPooling1D(pool_length=3, border_mode='valid'), name='Mpool4', input='relu4')

        self.model.add_node(Convolution1D(16, 15, border_mode='same'), name='conv5', input='Mpool4')
        self.model.add_node(BatchNormalization(mode=2), name='batch_norm5', input='conv5')
        self.model.add_node(Activation('relu'), name='relu5', input='batch_norm5')
        self.model.add_node(MaxPooling1D(pool_length=3, border_mode='valid'), name='Mpool5', input='relu5')

        #self.model.add_node(Reshape((16 * 18,)), name='reshape1', input='Mpool5')
        self.model.add_node(Flatten(), name='reshape1', input='Mpool5')

        self.model.add_node(Dense(8, activation='relu'), name='dense1', input='reshape1')
        self.model.add_node(Dropout(0.5), name='dropout1', input='dense1')
        self.model.add_node(Dense(8, activation='relu'), name='dense2', input='dropout1')
        self.model.add_node(Dropout(0.5), name='dropout2', input='dense2')
        self.model.add_node(Dense(2, activation='softmax'), name='softmax', input='dropout2')
        self.model.add_output(name='output', input='softmax')
        adam = Adam(lr=0.0001, beta_1=0.9, beta_2=0.999, epsilon=1e-08)
        self.model.compile(adam, {'output': 'categorical_crossentropy'})
        #self.model.compile('adam', {'output': 'categorical_crossentropy'})

        self.logger.debug('model is successfully created.')

class CNN1DSimple(CNN1DModel):
    def create(self):
        num_column = int(self.opt['num_column'])

        self.model = Graph()
        self.model.add_input(name='input1', input_shape=(num_column,1))
        self.model.add_input(name='input2', input_shape=(num_column,1))
    
        self.model.add_node(Convolution1D(8, 10, border_mode='same',init = 'he_uniform'), name = 'conv1', inputs = ['input1','input2'])
        self.model.add_node(BatchNormalization(mode=2),name = 'batch_norm1', input = 'conv1')
        self.model.add_node(Activation('relu'), name = 'relu1', input = 'batch_norm1')
        self.model.add_node(MaxPooling1D(pool_length=2, border_mode='valid'), name = 'Mpool1', input = 'relu1')
    
        self.model.add_node(Convolution1D(16, 10, border_mode='same',init = 'he_uniform'), name = 'conv2', input= 'Mpool1')
        self.model.add_node(BatchNormalization(mode=2),name = 'batch_norm2', input = 'conv2')
        self.model.add_node(Activation('relu'), name = 'relu2', input = 'batch_norm2')
        self.model.add_node(MaxPooling1D(pool_length=2, border_mode='valid'), name = 'Mpool2', input = 'relu2')

        self.model.add_node(Convolution1D(24, 10, border_mode='same',init = 'he_uniform'), name = 'conv3', input= 'Mpool2')
        self.model.add_node(BatchNormalization(mode=2),name = 'batch_norm3', input = 'conv3')
        self.model.add_node(Activation('relu'), name = 'relu3', input = 'batch_norm3')
        self.model.add_node(MaxPooling1D(pool_length=2, border_mode='valid'), name = 'Mpool3', input = 'relu3')
    
        self.model.add_node(Convolution1D(32, 10, border_mode='same',init = 'he_uniform'), name = 'conv4', input= 'Mpool3')
        self.model.add_node(BatchNormalization(mode=2),name = 'batch_norm4', input = 'conv4')
        self.model.add_node(Activation('relu'), name = 'relu4', input = 'batch_norm4')
        self.model.add_node(MaxPooling1D(pool_length=2, border_mode='valid'), name = 'Mpool4', input = 'relu4')

        self.model.add_node(Flatten(), name='flatten', input = 'Mpool4')
        self.model.add_node(Dense(32,init = 'he_uniform'), name='fully1', input='flatten')
        self.model.add_node(Activation('relu'), name = 'fully_relu1', input = 'fully1')
        self.model.add_node(Dropout(0.5), name = 'fully_dropout1', input = 'fully_relu1')
        self.model.add_node(Dense(32,init = 'he_uniform'), name='fully2', input='fully_dropout1')
        self.model.add_node(Activation('relu'), name = 'fully_relu2', input = 'fully2')
        self.model.add_node(Dropout(0.5), name = 'fully_dropout2', input = 'fully_relu2')
        self.model.add_node(Dense(1 , activation = 'sigmoid',init = 'he_uniform'), name='softmax', input='fully_dropout2')
        self.model.add_output(name='output', input='softmax')
        #self.model = make_parallel(self.model, 2)
        adam = Adam(lr=0.00001, beta_1=0.9, beta_2=0.999, epsilon=1e-08)
        self.model.compile(adam, {'output': 'binary_crossentropy'})
        self.logger.debug('model is successfully created.')

    def fit(self, train_data=None, weights=None, opt={}):
        num_column = int(self.opt['num_column'])

        if weights is not None:
            self.model.load_weights(weights)
        else:
            batch_size = 256 if 'batch_size' not in opt else int(opt['batch_size'])
            nb_epoch = 1 if 'nb_epoch' not in opt else int(opt['nb_epoch'])
            show_accuracy = True if 'show_accuracy' not in opt else bool(opt['show_accuracy'])
            verbose = 1 if 'verbose' not in opt else int(opt['verbose'])

            self.model.fit({
                'input1': (np.array(train_data['X'])[:, 0:num_column]).reshape(train_data['X'].shape[0], num_column, 1),
                'input2': (np.array(train_data['X'])[:, num_column:(num_column*2)]).reshape(train_data['X'].shape[0], num_column, 1),
                'output': np.array(train_data['Y'])},
                batch_size=batch_size, nb_epoch=nb_epoch, show_accuracy=show_accuracy, verbose=verbose)

    def predict(self, input_data, opt={}):
        num_column = int(self.opt['num_column'])
        batch_size = 128 if 'batch_size' not in opt else int(opt['batch_size'])
        verbose = 1 if 'verbose' not in opt else int(opt['verbose'])

        predicted = self.model.predict({
            'input1': (np.array(input_data['X'])[:, 0:num_column]).reshape(input_data['X'].shape[0], num_column, 1),
            'input2': (np.array(input_data['X'])[:, num_column:(num_column*2)]).reshape(input_data['X'].shape[0], num_column, 1)},
            verbose=verbose, batch_size=batch_size)
        #print predicted
        predicted = predicted['output'].tolist()
        predicted = sum(predicted, [])
        #print( predicted )
        #print( predicted.shape )
        null_array = [0.]*len(predicted)

        return np.transpose(np.array([null_array,predicted]))


class CNN1D1Min_1_1_0(CNN1DModel):
    def create(self):
        num_column = int(self.opt['num_column'])

        self.model = Graph()
        self.model.add_input(name='input1', input_shape=(num_column, 1))
        self.model.add_input(name='input2', input_shape=(num_column, 1))

        self.model.add_node(Convolution1D(2, 15, border_mode='same'), name='conv1', inputs=['input1', 'input2'])
        #self.model.add_node(BatchNormalization(), name='batch_norm1', input='conv1')
        self.model.add_node(Activation('relu'), name='relu1', input='conv1')
        self.model.add_node(MaxPooling1D(pool_length=3, border_mode='valid'), name='Mpool1', input='relu1')

        self.model.add_node(Convolution1D(4, 15, border_mode='same'), name='conv2', input='Mpool1')
        #self.model.add_node(BatchNormalization(), name='batch_norm2', input='conv2')
        self.model.add_node(Activation('relu'), name='relu2', input='conv2')
        self.model.add_node(MaxPooling1D(pool_length=3, border_mode='valid'), name='Mpool2', input='relu2')

        self.model.add_node(Convolution1D(8, 15, border_mode='same'), name='conv3', input='Mpool2')
        #self.model.add_node(BatchNormalization(), name='batch_norm3', input='conv3')
        self.model.add_node(Activation('relu'), name='relu3', input='conv3')
        self.model.add_node(MaxPooling1D(pool_length=3, border_mode='valid'), name='Mpool3', input='relu3')

        self.model.add_node(Convolution1D(16, 15, border_mode='same'), name='conv4', input='Mpool3')
        #self.model.add_node(BatchNormalization(), name='batch_norm4', input='conv4')
        self.model.add_node(Activation('relu'), name='relu4', input='conv4')
        self.model.add_node(MaxPooling1D(pool_length=3, border_mode='valid'), name='Mpool4', input='relu4')

        self.model.add_node(Flatten(), name='reshape1', input='Mpool4')
        self.model.add_node(Dense(8, activation='relu'), name='dense1', input='reshape1')
        self.model.add_node(Dropout(0.5), name='dropout1', input='dense1')
        self.model.add_node(Dense(8, activation='relu'), name='dense2', input='dropout1')
        self.model.add_node(Dropout(0.5), name='dropout2', input='dense2')
        self.model.add_node(Dense(2, activation='softmax'), name='softmax', input='dropout2')
        self.model.add_output(name='output', input='softmax')
        #adam = Adam(lr=0.0001, beta_1=0.9, beta_2=0.999, epsilon=1e-08)
        #
        #self.model.compile(adam, {'output': 'categorical_crossentropy'})
        adam = Adam(lr=0.000005, beta_1=0.9, beta_2=0.999, epsilon=1e-08)
        self.model.compile(adam, {'output': 'mean_absolute_error'})
        #self.model.compile('adam', {'output': 'mean_absolute_error'})

        self.logger.debug('model is successfully created.')


class CNN1DLSTM(CNN1DModel):
    def create(self):
        num_column = int(self.opt['num_column'])

        self.model = Graph()
        self.model.add_input(name='input1', input_shape=(num_column, 1))
        self.model.add_input(name='input2', input_shape=(num_column, 1))

        self.model.add_node(Convolution1D(2, 15, border_mode='same', input_shape=(num_column, 1)), name='conv1',
                            input='input1')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool1', input='conv1')
        self.model.add_node(Convolution1D(4, 5, border_mode='same', input_shape=(num_column, 1)), name='conv2',
                            input='Mpool1')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool2', input='conv2')
        self.model.add_node(Convolution1D(8, 3, border_mode='same', input_shape=(num_column, 1)), name='conv3',
                            input='Mpool2')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool3', input='conv3')
        self.model.add_node(Convolution1D(16, 3, border_mode='same', input_shape=(num_column, 1)), name='conv4',
                            input='Mpool3')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool4', input='conv4')

        self.model.add_node(Convolution1D(2, 15, border_mode='same', input_shape=(num_column, 1)), name='conv11',
                            input='input2')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool11', input='conv11')
        self.model.add_node(Convolution1D(4, 5, border_mode='same', input_shape=(num_column, 1)), name='conv22',
                            input='Mpool11')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool22', input='conv22')
        self.model.add_node(Convolution1D(8, 3, border_mode='same', input_shape=(num_column, 1)), name='conv33',
                            input='Mpool22')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool33', input='conv33')
        self.model.add_node(Convolution1D(16, 3, border_mode='same', input_shape=(num_column, 1)), name='conv44',
                            input='Mpool33')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool44', input='conv44')

        self.model.add_node(LSTM(32), name='forward1', inputs=['Mpool4', 'Mpool44'])
        self.model.add_node(LSTM(32, go_backwards=True), name='backward1', inputs=['Mpool4', 'Mpool44'])
        self.model.add_node(Dropout(0.5), name='dropout1', inputs=['forward1', 'backward1'])
        self.model.add_node(Dense(2, activation='softmax'), name='softmax', input='dropout1')

        self.model.add_output(name='output', input='softmax')
        adam = Adam(lr=0.000005, beta_1=0.9, beta_2=0.999, epsilon=1e-08)
        self.model.compile(adam, {'output': 'mean_absolute_error'})
        #self.model.compile('adam', {'output': 'categorical_crossentropy'})
        self.logger.debug('model is successfully created.')

class CNN1DLSTM2Ch(CNN1DModel):
    def create(self):
        num_column = int(self.opt['num_column'])

        self.model = Graph()
        self.model.add_input(name='input1', input_shape=(num_column, 1))
        self.model.add_input(name='input2', input_shape=(num_column, 1))
        self.model.add_input(name='input3', input_shape=(num_column, 1))
        self.model.add_input(name='input4', input_shape=(num_column, 1))

        self.model.add_node(Convolution1D(32, 15, border_mode='same', input_shape=(num_column, 1)), name='conv1',
                            input='input1')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool1', input='conv1')
        self.model.add_node(Convolution1D(128, 5, border_mode='same', input_shape=(num_column, 1)), name='conv2',
                            input='Mpool1')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool2', input='conv2')
        self.model.add_node(Convolution1D(256, 3, border_mode='same', input_shape=(num_column, 1)), name='conv3',
                            input='Mpool2')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool3', input='conv3')
        self.model.add_node(Convolution1D(512, 3, border_mode='same', input_shape=(num_column, 1)), name='conv4',
                            input='Mpool3')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool4', input='conv4')

        self.model.add_node(Convolution1D(32, 15, border_mode='same', input_shape=(num_column, 1)), name='conv11',
                            input='input2')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool11', input='conv11')
        self.model.add_node(Convolution1D(128, 5, border_mode='same', input_shape=(num_column, 1)), name='conv22',
                            input='Mpool11')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool22', input='conv22')
        self.model.add_node(Convolution1D(256, 3, border_mode='same', input_shape=(num_column, 1)), name='conv33',
                            input='Mpool22')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool33', input='conv33')
        self.model.add_node(Convolution1D(512, 3, border_mode='same', input_shape=(num_column, 1)), name='conv44',
                            input='Mpool33')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool44', input='conv44')

        self.model.add_node(Convolution1D(32, 15, border_mode='same', input_shape=(num_column, 1)), name='conv111',
                            input='input3')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool111', input='conv111')
        self.model.add_node(Convolution1D(128, 5, border_mode='same', input_shape=(num_column, 1)), name='conv222',
                            input='Mpool111')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool222', input='conv222')
        self.model.add_node(Convolution1D(256, 3, border_mode='same', input_shape=(num_column, 1)), name='conv333',
                            input='Mpool222')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool333', input='conv333')
        self.model.add_node(Convolution1D(512, 3, border_mode='same', input_shape=(num_column, 1)), name='conv444',
                            input='Mpool333')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool444', input='conv444')

        self.model.add_node(Convolution1D(32, 15, border_mode='same', input_shape=(num_column, 1)), name='conv1111',
                            input='input4')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool1111',
                            input='conv1111')
        self.model.add_node(Convolution1D(128, 5, border_mode='same', input_shape=(num_column, 1)), name='conv2222',
                            input='Mpool1111')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool2222',
                            input='conv2222')
        self.model.add_node(Convolution1D(256, 3, border_mode='same', input_shape=(num_column, 1)), name='conv3333',
                            input='Mpool2222')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool3333',
                            input='conv3333')
        self.model.add_node(Convolution1D(512, 3, border_mode='same', input_shape=(num_column, 1)), name='conv4444',
                            input='Mpool3333')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool4444',
                            input='conv4444')

        self.model.add_node(LSTM(128), name='forward1', inputs=['Mpool4', 'Mpool44', 'Mpool444', 'Mpool4444'])
        self.model.add_node(LSTM(128, go_backwards=True), name='backward1', inputs=['Mpool4', 'Mpool44', 'Mpool444', 'Mpool4444'])
        self.model.add_node(Dropout(0.5), name='dropout1', inputs=['forward1', 'backward1'])
        self.model.add_node(Dense(2, activation='softmax'), name='softmax', input='dropout1')

        self.model.add_output(name='output', input='softmax')

        # self.model.compile('adam', {'output': 'categorical_crossentropy'})
        adam = Adam(lr=0.000005, beta_1=0.9, beta_2=0.999, epsilon=1e-08)
        self.model.compile(adam, {'output': 'mean_absolute_error'})

    def fit(self, train_data=None, weights=None, opt={}):
        num_column = int(self.opt['num_column'])

        import os.path
        if weights is not None and os.path.exists(weights):
            self.model.load_weights(weights)
        else:
            batch_size = 256 if 'batch_size' not in opt else int(opt['batch_size'])
            nb_epoch = 1 if 'nb_epoch' not in opt else int(opt['nb_epoch'])
            show_accuracy = True if 'show_accuracy' not in opt else bool(opt['show_accuracy'])
            verbose = 1 if 'verbose' not in opt else int(opt['verbose'])

            self.model.fit({
                'input1': (np.array(train_data['X'])[:, 0:num_column]).reshape(train_data['X'].shape[0], num_column, 1),
                'input2': (np.array(train_data['X'])[:, num_column:(num_column*2)]).reshape(train_data['X'].shape[0], num_column, 1),
                'input3': (np.array(train_data['X'])[:, (num_column*2):(num_column * 3)]).reshape(train_data['X'].shape[0],
                                                                                              num_column, 1),
                'input4': (np.array(train_data['X'])[:, (num_column*3):(num_column * 4)]).reshape(train_data['X'].shape[0],
                                                                                              num_column, 1),
                'output': np_utils.to_categorical(np.array(train_data['Y']), 2)},
                batch_size=batch_size, nb_epoch=nb_epoch, show_accuracy=show_accuracy, verbose=verbose)
            if weights is not None:
                model.save_weights(weights,overwrite=True)

    def predict(self, input_data, opt={}):
        num_column = int(self.opt['num_column'])
        batch_size = 128 if 'batch_size' not in opt else int(opt['batch_size'])
        verbose = 1 if 'verbose' not in opt else int(opt['verbose'])

        predicted = self.model.predict({
            'input1': (np.array(input_data['X'])[:, 0:num_column]).reshape(input_data['X'].shape[0], num_column, 1),
            'input2': (np.array(input_data['X'])[:, num_column:(num_column*2)]).reshape(input_data['X'].shape[0], num_column, 1),
            'input3': (np.array(input_data['X'])[:, (num_column*2):(num_column * 3)]).reshape(input_data['X'].shape[0],
                                                                                          num_column, 1),
            'input4': (np.array(input_data['X'])[:, (num_column*3):(num_column * 4)]).reshape(input_data['X'].shape[0],
                                                                                          num_column, 1),
        },
            verbose=verbose, batch_size=batch_size)
        #print predicted
        predicted = predicted['output'].tolist()

        return predicted


class CNN1DLSTM2ChSimple(CNN1DLSTM2Ch):
    def create(self):
        num_column = int(self.opt['num_column'])

        self.model = Graph()
        self.model.add_input(name='input1', input_shape=(num_column, 1))
        self.model.add_input(name='input2', input_shape=(num_column, 1))
        self.model.add_input(name='input3', input_shape=(num_column, 1))
        self.model.add_input(name='input4', input_shape=(num_column, 1))

        self.model.add_node(Convolution1D(4, 15, border_mode='same', input_shape=(num_column, 1)), name='conv1',
                            input='input1')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool1', input='conv1')
        self.model.add_node(Convolution1D(8, 5, border_mode='same', input_shape=(num_column, 1)), name='conv2',
                            input='Mpool1')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool2', input='conv2')
        self.model.add_node(Convolution1D(16, 3, border_mode='same', input_shape=(num_column, 1)), name='conv3',
                            input='Mpool2')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool3', input='conv3')
        self.model.add_node(Convolution1D(32, 3, border_mode='same', input_shape=(num_column, 1)), name='conv4',
                            input='Mpool3')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool4', input='conv4')

        self.model.add_node(Convolution1D(4, 15, border_mode='same', input_shape=(num_column, 1)), name='conv11',
                            input='input2')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool11', input='conv11')
        self.model.add_node(Convolution1D(8, 5, border_mode='same', input_shape=(num_column, 1)), name='conv22',
                            input='Mpool11')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool22', input='conv22')
        self.model.add_node(Convolution1D(16, 3, border_mode='same', input_shape=(num_column, 1)), name='conv33',
                            input='Mpool22')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool33', input='conv33')
        self.model.add_node(Convolution1D(32, 3, border_mode='same', input_shape=(num_column, 1)), name='conv44',
                            input='Mpool33')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool44', input='conv44')

        self.model.add_node(Convolution1D(4, 15, border_mode='same', input_shape=(num_column, 1)), name='conv111',
                            input='input3')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool111', input='conv111')
        self.model.add_node(Convolution1D(8, 5, border_mode='same', input_shape=(num_column, 1)), name='conv222',
                            input='Mpool111')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool222', input='conv222')
        self.model.add_node(Convolution1D(16, 3, border_mode='same', input_shape=(num_column, 1)), name='conv333',
                            input='Mpool222')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool333', input='conv333')
        self.model.add_node(Convolution1D(32, 3, border_mode='same', input_shape=(num_column, 1)), name='conv444',
                            input='Mpool333')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool444', input='conv444')

        self.model.add_node(Convolution1D(4, 15, border_mode='same', input_shape=(num_column, 1)), name='conv1111',
                            input='input4')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool1111',
                            input='conv1111')
        self.model.add_node(Convolution1D(8, 5, border_mode='same', input_shape=(num_column, 1)), name='conv2222',
                            input='Mpool1111')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool2222',
                            input='conv2222')
        self.model.add_node(Convolution1D(16, 3, border_mode='same', input_shape=(num_column, 1)), name='conv3333',
                            input='Mpool2222')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool3333',
                            input='conv3333')
        self.model.add_node(Convolution1D(32, 3, border_mode='same', input_shape=(num_column, 1)), name='conv4444',
                            input='Mpool3333')
        self.model.add_node(MaxPooling1D(pool_length=3, stride=3, border_mode='valid'), name='Mpool4444',
                            input='conv4444')

        self.model.add_node(LSTM(32), name='forward1', inputs=['Mpool4', 'Mpool44', 'Mpool444', 'Mpool4444'])
        self.model.add_node(LSTM(32, go_backwards=True), name='backward1', inputs=['Mpool4', 'Mpool44', 'Mpool444', 'Mpool4444'])
        self.model.add_node(Dropout(0.5), name='dropout1', inputs=['forward1', 'backward1'])
        self.model.add_node(Dense(2, activation='softmax'), name='softmax', input='dropout1')

        self.model.add_output(name='output', input='softmax')

        # self.model.compile('adam', {'output': 'categorical_crossentropy'})
        adam = Adam(lr=0.000001, beta_1=0.9, beta_2=0.999, epsilon=1e-08)
        self.model.compile(adam, {'output': 'mean_absolute_error'})

class LSTMModel2Ch(CNN1DLSTM2Ch):
    def create(self):
        num_column = int(self.opt['num_column'])

        self.model = Graph()
        self.model.add_input(name='input1', input_shape=(num_column, 1))
        self.model.add_input(name='input2', input_shape=(num_column, 1))
        self.model.add_input(name='input3', input_shape=(num_column, 1))
        self.model.add_input(name='input4', input_shape=(num_column, 1))
        self.model.add_node(LSTM(32), name='forward1', inputs=['input1', 'input2', 'input3','input4'])
        self.model.add_node(LSTM(32, go_backwards=True), name='backward1', inputs=['input1', 'input2', 'input3','input4'])
        self.model.add_node(Dropout(0.5), name='dropout1', inputs=['forward1', 'backward1'])
        self.model.add_node(Dense(2, activation='softmax'), name='softmax', input='dropout1')
        self.model.add_output(name='output', input='softmax')
        self.model.compile('adam', {'output': 'categorical_crossentropy'})

        self.logger.debug('model is successfully created.')


def evaluate(input_data, predicted, threshold=0.60, serial='9999999', output_dir=None, time_start=0,time_end=2000000000):
    X_measured = map(lambda x: 1 if float(x)>0.0 else 0, np.array(input_data['Y']))
    X_predicted = map(lambda x: 1 if x[1]>threshold else 0, np.array(predicted))

    out = pd.DataFrame({'serial': np.repeat(serial, len(input_data['ts'])), #input_data['id'].flatten(), #np.repeat(serial, len(input_data['ts'])),
                        'ts': input_data['ts'].flatten(),
                        'plug': X_measured,
                        'nilm': X_predicted})
    out = out[['serial', 'ts', 'plug', 'nilm']]

    #log = open(output_dir + '/output.txt','a')
    #ids = np.unique(out.id)
    #print ids
    #print id
    out = out[(out.ts > time_start) & (out.ts < time_end)]
    out.sort(['ts'])
    #print one_out
    #out = out.query()

    if output_dir is not None:
        #print '===== writing output file ====='
        out.to_csv(output_dir + '/' + str(serial) + '_eval.csv',index=False,header=True)

    #avg_p = (metrics.average_precision_score(np.array(X_measured), np.array(X_predicted)))
    #pre = (metrics.precision_score(np.array(X_measured), np.array(X_predicted)))
    #rec = (metrics.recall_score(np.array(X_measured), np.array(X_predicted)))
    #f1 = (metrics.f1_score(np.array(X_measured), np.array(X_predicted)))

    avg_p = (metrics.average_precision_score(np.array(out.plug), np.array(out.nilm)))
    pre = (metrics.precision_score(np.array(out.plug), np.array(out.nilm)))
    rec = (metrics.recall_score(np.array(out.plug), np.array(out.nilm)))
    f1 = (metrics.f1_score(np.array(out.plug), np.array(out.nilm)))
    #auroc = (metrics.roc_auc_score(np.array(X_measured), np.array(predicted)[:,0:1]))

    output_stat = "{0:s}, {1:.3f}, {2:.3f}, {3:.3f}, {4:.3f}\n".format(serial,avg_p, pre, rec, f1)
    #log.write(output_stat)
    #print output_stat
    return output_stat

import math
def postprocess_time_merge(ts, predicted, t1=0.96, t2=1.0):
    i = 0
    prev_window = 0
    bucket = []
    processed = []
    for t in range(0,len(ts)+1):
        curr_time = ts[min(t,len(ts)-1)]
        curr_window = int(math.floor(curr_time/900.))
        if (t == len(ts) or prev_window != curr_window) and len(bucket)>0:
            bucket_len = len(bucket)
            if np.sum(map(lambda x: 1 if x>t1 else 0,bucket))/float(bucket_len) > t2:
                out_category = 1.0
            else:
                out_category = 0.0
            processed = processed + [out_category]*bucket_len

            #reset
            bucket = []
            if t == len(ts):
                break

        bucket.append(predicted[t][1])
        prev_window = curr_window


    return np.array([[0]*len(processed),processed]).transpose()
   
