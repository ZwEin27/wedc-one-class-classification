# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-11 14:17:25
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-11 15:18:25



class Labeller(object):

    @staticmethod
    def label(unlabelled_data_path, labelled_data_path):
        reader = csv.reader(codecs.open(unlabelled_data_path, 'r'))
        # print reader.fieldnames
        header = next(reader)
        dataset = []
        for row in reader:
            print row

        with open(path, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['label']+header)
            writer.writeheader()
            for data in dataset:
                writer.writerow(data)


if __name__ == '__main__':
    from loader import *
    Labeller.label(DC_DEFAULT_UNLABELLED_DATA_FILEPATH, DC_DEFAULT_LABELLED_DATA_FILEPATH)
