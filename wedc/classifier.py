# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-09 11:36:55
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-09 13:25:30

from loader import Loader

class Classifier(object):

    def __init__(self, training_data_file_path, testing_data_file_path=None, predicting_data_file_path=None):
        self._training_data_file_path = training_data_file_path
        self._testing_data_file_path = testing_data_file_path
        self._predicting_data_file_path = predicting_data_file_path

        self._training_data = Loader.load_training_data(filepath=training_data_file_path)



    def train(self):
        pass

    def test(self):
        pass

    def predict(self):
        pass


