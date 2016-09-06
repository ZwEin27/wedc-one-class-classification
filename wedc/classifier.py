# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-09 11:36:55
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-09-07 20:15:30

from loader import Loader
import json
import os
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
            DC_CATEGORY_NAME_MASSAGE: svm.OneClassSVM(nu=0.1, kernel="rbf", gamma=0.1, max_iter=1000, tol=0.0001),
            DC_CATEGORY_NAME_ESCORT: svm.OneClassSVM(nu=0.1, kernel="rbf", gamma=0.1),
            DC_CATEGORY_NAME_JOB_ADS: svm.OneClassSVM(nu=0.1, kernel="rbf", gamma=1./10, max_iter=1000, tol=0.0001)
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

    def evaluate(self, train_test_split_rate=.8, random_seed=30):
        for (cate_name, cate_no) in DC_CATEGORY_NO_MAPPING.iteritems():
            print 'evaluate', cate_name, 'with label', cate_no
            print '-'*40
            inner_data = Loader.load_vectors([_ for _ in self._training_data if _._label == cate_no])
            outer_data = Loader.load_vectors([_ for _ in self._training_data if _._label != cate_no])

            inner_data_index = [i for i in range(len(self._training_data)) if self._training_data[i]._label == cate_no]
            outer_data_index = [i for i in range(len(self._training_data)) if self._training_data[i]._label != cate_no]

            # print 'inner_data_index:', inner_data_index
            # print 'outer_data_index:', outer_data_index

            if not inner_data:
                continue

            np.random.seed(random_seed)
            np.random.shuffle(inner_data)
            split_point = int(len(inner_data)*train_test_split_rate)
            train_data = inner_data[:split_point]
            test_data = inner_data[split_point:]
            outlier_data = outer_data

            print 'total ground truth:', len(inner_data), \
                ', train_data:', len(train_data), \
                ', test_data:', len(test_data), \
                ', outlier_data:', len(outlier_data)
            
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

            n_true_train = y_pred_train[y_pred_train == 1].size
            n_true_test = y_pred_test[y_pred_test == 1].size
            n_true_outliers = y_pred_outliers[y_pred_outliers == -1].size

            # print 'error_test index', [inner_data_index[split_point:][_] for _ in [i for i in range(len(y_pred_test)) if y_pred_test[i] == -1]]
            # print 'error_outlier index', [outer_data_index[_] for _ in [i for i in range(len(y_pred_outliers)) if y_pred_outliers[i] == -1]]

            print 'error_train:', str(n_error_train)+'/'+str(len(train_data)), \
                ', error_test:', str(n_error_test)+'/'+str(len(test_data)), \
                ', error_outlier:', str(n_error_outliers)+'/'+str(len(outlier_data))

            print 'precision:', str(n_true_test)+'/'+str(n_true_test+n_error_outliers)
            print 'recall:', str(n_true_test)+'/'+str(len(test_data))



            # print cate_name+': training data:', json.dumps([[_._content for _ in self._training_data if _._label == cate_no][i] for i in range(len(list(y_pred_train))) if list(y_pred_train)[i] == 1], indent=4)
            # # """
            # print cate_name+': error_train:', json.dumps([[_._content for _ in self._training_data if _._label == cate_no][i] for i in range(len(list(y_pred_train))) if list(y_pred_train)[i] == -1], indent=4)
            # print cate_name+': error_test:', json.dumps([[_._content for _ in self._training_data if _._label == cate_no][i+len(train_data)] for i in range(len(list(y_pred_test))) if list(y_pred_test)[i] == -1], indent=4)
            # print cate_name+': error_outlier:', json.dumps([[_._content for _ in self._training_data if _._label != cate_no][i] for i in range(len(list(y_pred_outliers))) if list(y_pred_outliers)[i] == 1], indent=4)
            # # """
            
            # print 'error_train:', json.dumps([train_data[i] for i in range(len(list(y_pred_train))) if list(y_pred_train)[i] == -1], indent=4)
            # print 'error_test:', json.dumps([test_data[i] for i in range(len(list(y_pred_test))) if list(y_pred_test)[i] == -1], indent=4)
            # print 'error_outlier:', json.dumps([outlier_data[i] for i in range(len(list(y_pred_outliers))) if list(y_pred_outliers)[i] == 1], indent=4)

            print '\n\n\n'
            # break

    def predict(self):
        pass

if __name__ == '__main__':
    # classifier = Classifier().train()
    # classifier = Classifier().evaluate()
    classifier = Classifier(training_data_file_path=os.path.join(os.path.dirname(__file__), 'res', 'training_data.csv')).evaluate()
