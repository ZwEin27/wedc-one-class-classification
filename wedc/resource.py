# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-09-06 11:30:37
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-09-07 18:43:26

import re
import os
import sys
import csv

def do_filter(path, keyword):

    # re_filter = re.compile(r'.*'+keyword+'.*', re.IGNORECASE)
    # no work
    import csv
    dataset = []
    header = None
    with open(path, 'rb') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        # dataset.append(','.join(header)+'\n')
        for row in reader:
            # label =row[0]
            # content = row[1]#.decode('utf-8', 'ignore').encode('ascii', 'ignore')
            
            data = {}
            for i in range(len(header)):
                data.setdefault(header[i], row[i])
            # data['label'] = 4
            if keyword in ' '.join(row):
                continue

            # if len(re_filter.findall(content)) > 0:
            #     continue



            dataset.append(data)
    # print header
    with open(path, 'w') as csvfile:
        # print header_content
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()        
        for data in dataset:
            writer.writerow(data)

def generate_training_data():

    paths = [
        os.path.join(os.path.dirname(__file__), 'res', 'massage.csv'),
        os.path.join(os.path.dirname(__file__), 'res', 'escort.csv'),
        os.path.join(os.path.dirname(__file__), 'res', 'job_ads.csv')
    ]

    dataset = []
    header_content = None

    for path in paths:
        print 'combine by path at', path
        with open(path, 'rb') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)
            if not header_content:
                header_content = header # ','.join(header)
            for row in reader:
                data = {}
                for i in range(len(header)):
                    data.setdefault(header[i], row[i])
                dataset.append(data)

    training_data_path = os.path.join(os.path.dirname(__file__), 'res', 'training_data.csv')

    with open(training_data_path, 'w') as csvfile:
        # print header_content
        writer = csv.DictWriter(csvfile, fieldnames=header_content)
        writer.writeheader()        
        for data in dataset:
            writer.writerow(data)


if __name__ == '__main__':

    # # do filter
    # input_filename = os.path.join(os.path.dirname(__file__), 'res', 'job_ads.csv')
    # keyword = 'erotic'
    # do_filter(input_filename, keyword)
    

    generate_training_data()





