# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-08 11:46:11
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-09 13:15:40


import os
import csv

DC_DATA_FILE_FORMAT_JSON = 'json'
DC_DATA_FILE_FORMAT_CSV = 'csv'

DC_DATA_FILE_FORMATS = [
    DC_DATA_FILE_FORMAT_JSON,
    DC_DATA_FILE_FORMAT_CSV
]


DC_DEFAULT_TRAINING_DATA_FILEPATH = os.path.join(os.path.dirname(__file__), 'res', 'training_data.csv')
DC_DEFAULT_TESTING_DATA_FILEPATH = os.path.join(os.path.dirname(__file__), 'res', 'testing_data.csv')


class Loader(object):

    #################################################
    # Basic Load Data Functions
    #################################################

    def __load_data_jsonlines(path):
        import jsonlines
        dataset = []
        lines = jsonlines.open(path, mode='r')
        for line in lines:
            dataset.append([line['sid'], line['content']])
        return dataset

    def __load_data_csv(path):
        import csv
        dataset = []
        with open(path, 'rb') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                dataset.append(row)
        return dataset

    __loader_data_funcs = {
        DC_DATA_FILE_FORMAT_JSON: __load_data_jsonlines,
        DC_DATA_FILE_FORMAT_CSV: __load_data_csv
    }

    @staticmethod
    def load_data(path, format='jsonlines'):
        """
        basic interface to load data
        
        """
        return Loader.__loader_data_funcs[format](path)

    #################################################
    # Main Load Data Functions
    #################################################
    
    @staticmethod
    def load_training_data(filepath=DC_DEFAULT_TRAINING_DATA_FILEPATH):
        return Loader.load_data(filepath, format='csv')

    @staticmethod
    def load_testing_data(filepath=DC_DEFAULT_TESTING_DATA_FILEPATH):
        return Loader.load_data(filepath, format='csv')

       

print Loader.load_training_data()




