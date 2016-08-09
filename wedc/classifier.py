# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-09 11:36:55
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-09 15:40:52

from loader import Loader

import numpy as np
from sklearn import svm

DC_CATEGORY_NAME_MASSAGE = 'massage'
DC_CATEGORY_NAME_ESCORT = 'escort'
DC_CATEGORY_NAME_JOB_ADS = 'job_ads'

DC_CATEGORY_NO_MAPPING = {
    DC_CATEGORY_NAME_MASSAGE: 2,
    DC_CATEGORY_NAME_ESCORT: 3,
    DC_CATEGORY_NAME_JOB_ADS: 4
}

class Classifier(object):


    def __init__(self, training_data_file_path=None, testing_data_file_path=None, predicting_data_file_path=None):
        self._training_data_file_path = training_data_file_path
        self._testing_data_file_path = testing_data_file_path
        self._predicting_data_file_path = predicting_data_file_path

        self._training_data = Loader.load_training_data(filepath=training_data_file_path) if self._training_data_file_path else Loader.load_training_data()

        self.clf = svm.OneClassSVM(nu=0.1, kernel="rbf", gamma=0.1)


    def train(self):
        X_train = Loader.load_vectors(self._training_data)
        X_train = np.array(np.mat(';'.join([_ for _ in X_train])))
        self.clf.fit(X_train)
        y_pred_train = self.clf.predict(X_train)
        print y_pred_train

    def test(self):
        pass

    def predict(self):
        pass


if __name__ == '__main__':
    classifier = Classifier().train()