# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-09 11:36:55
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-09 20:32:23

from loader import Loader

import numpy as np
from sklearn import svm

DC_CATEGORY_NAME_MASSAGE = 'massage'
DC_CATEGORY_NAME_ESCORT = 'escort'
DC_CATEGORY_NAME_JOB_ADS = 'job_ads'

DC_CATEGORY_NO_MAPPING = {
    DC_CATEGORY_NAME_MASSAGE: '2',
    DC_CATEGORY_NAME_ESCORT: '3',
    DC_CATEGORY_NAME_JOB_ADS: '4'
}

class Classifier(object):


    def __init__(self, training_data_file_path=None, predicting_data_file_path=None):
        self._training_data_file_path = training_data_file_path
        # self._testing_data_file_path = testing_data_file_path
        self._predicting_data_file_path = predicting_data_file_path

        self._training_data = Loader.load_training_data(filepath=training_data_file_path) if self._training_data_file_path else Loader.load_training_data()

        self.classifiers = {
            DC_CATEGORY_NAME_MASSAGE: svm.OneClassSVM(nu=0.1, kernel="rbf", gamma=0.1),
            DC_CATEGORY_NAME_ESCORT: svm.OneClassSVM(nu=0.1, kernel="rbf", gamma=0.1),
            DC_CATEGORY_NAME_JOB_ADS: svm.OneClassSVM(nu=0.1, kernel="rbf", gamma=0.1)
        }


    def train(self):
        for (cate_name, cate_no) in DC_CATEGORY_NO_MAPPING.iteritems():
            X_train = Loader.load_vectors([_ for _ in self._training_data if _._label == cate_no])
            if not X_train:
                continue
            X_train = np.array(np.mat(';'.join(X_train)))
            self.classifiers[cate_name].fit(X_train)

        # X_train = Loader.load_vectors(self._training_data)
        # X_train = np.array(np.mat(';'.join([_ for _ in X_train])))
        # self.clf.fit(X_train)
        # y_pred_train = self.clf.predict(X_train)
        # print y_pred_train

    def evaluate(self, train_test_split_rate=.8):
        for (cate_name, cate_no) in DC_CATEGORY_NO_MAPPING.iteritems():
            inner_data = Loader.load_vectors([_ for _ in self._training_data if _._label == cate_no])
            outer_data = Loader.load_vectors([_ for _ in self._training_data if _._label != cate_no])
            split_point = int(len(inner_data)*train_test_split_rate)
            train_data = inner_data[:split_point]
            test_data = inner_data[split_point:]
            outlier_data = outer_data
            
            if not train_data or not test_data or not outlier_data:
                print len(train_data), len(test_data), len(outlier_data)
                print 'not enough data to evaluate for', cate_name
                continue

            # transfer to numpy
            train_data = np.array(np.mat(';'.join([_ for _ in train_data])))
            test_data = np.array(np.mat(';'.join([_ for _ in test_data])))
            outlier_data = np.array(np.mat(';'.join([_ for _ in outlier_data])))

            self.classifiers[cate_name].fit(train_data)

            y_pred_train = self.classifiers[cate_name].predict(train_data)
            y_pred_test = self.classifiers[cate_name].predict(test_data)
            y_pred_outliers = self.classifiers[cate_name].predict(outlier_data)
            n_error_train = y_pred_train[y_pred_train == -1].size
            n_error_test = y_pred_test[y_pred_test == -1].size
            n_error_outliers = y_pred_outliers[y_pred_outliers == 1].size

        

    def predict(self):
        pass


if __name__ == '__main__':
    # classifier = Classifier().train()
    classifier = Classifier().evaluate()
