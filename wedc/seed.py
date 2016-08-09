# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-08 11:46:11
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-09 14:27:03

import os
import csv

DC_DEFAULT_SEED_FILEPATH = os.path.join(os.path.dirname(__file__), 'res', 'seed.csv')


class Seed(object):

    @staticmethod
    def load_seeds(filepath=DC_DEFAULT_SEED_FILEPATH):
        seeds = {}
        with open(filepath, 'rb') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                seeds.setdefault(row[0], row[1])
        return seeds

if __name__ == '__main__':

    print Seed.load_seeds()
