# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-08 11:46:11
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-17 13:07:13


import re
import os
import csv
import json
import codecs
from node import *

DC_LABEL_NAME = 'label'

DC_DATA_FILE_FORMAT_SEQUENCE = 'sequence'
DC_DATA_FILE_FORMAT_JSONLINES = 'jsonlines'
DC_DATA_FILE_FORMAT_JSON = 'json'
DC_DATA_FILE_FORMAT_CSV = 'csv'

DC_DATA_FILE_FORMATS = [
    DC_DATA_FILE_FORMAT_JSONLINES,
    DC_DATA_FILE_FORMAT_JSON,
    DC_DATA_FILE_FORMAT_CSV
]

DC_DEFAULT_DIG_WEBPAGE_DATA_FILEPATH = os.path.join(os.path.dirname(__file__), 'res', 'webpages.json')
DC_DEFAULT_UNLABELLED_DATA_FILEPATH = os.path.join(os.path.dirname(__file__), 'res', 'unlabelled_data.csv')
DC_DEFAULT_LABELLED_DATA_FILEPATH = os.path.join(os.path.dirname(__file__), 'res', 'labelled_data.csv')
DC_DEFAULT_TRAINING_DATA_FILEPATH = os.path.join(os.path.dirname(__file__), 'res', 'training_data.csv')
DC_DEFAULT_TESTING_DATA_FILEPATH = os.path.join(os.path.dirname(__file__), 'res', 'testing_data.csv')


class Loader(object):

    #################################################
    # Basic Load Data Functions
    #################################################

    def __load_data_sequence(path):
        # from pyspark import SparkContext, SparkConf, SparkFiles
        # from digSparkUtil.fileUtil import FileUtil
        # spark_config = SparkConf().setAppName('WEDC')
        # sc = SparkContext(conf=spark_config)
        # fUtil = FileUtil(sc)
        # rdd = fUtil.load_file(path, file_format='sequence', data_type='json')
        from hadoop.io import SequenceFile
        reader = SequenceFile.Reader(path)

        key_class = reader.getKeyClass()
        value_class = reader.getValueClass()

        key = key_class()
        value = value_class()

        #reader.sync(4042)
        position = reader.getPosition()
        while reader.next(key, value):
            print '*' if reader.syncSeen() else ' ',
            print '[%6s] %6s %6s' % (position, key.toString(), value.toString())
            position = reader.getPosition()
            break

        reader.close()

    def __load_data_jsonlines(path):
        # to be updated
        import jsonlines
        dataset = []
        lines = jsonlines.open(path, mode='r')
        for line in lines:
            dataset.append([line['sid'], line['content']])
        return dataset

    def __load_data_json(path):

        def es_content_loader(content):
            if not isinstance(content, basestring):
                content = ' '.join(content)
            return content

        dataset = []
        # json_objs = json.load(codecs.open(path, 'r', 'utf-8'))
        json_objs = json.load(codecs.open(path, 'r'))
        for json_obj in json_objs:
            source = json_obj['_source']

            doc_id = raw_content = posttime = city = text = region = title = userlocation = phonenumber = sid = otherads = age = None

            if DC_NODE_EXT_FEATURE_NAME_DOCID in source:
                doc_id = source[DC_NODE_EXT_FEATURE_NAME_DOCID]
            if DC_NODE_EXT_FEATURE_NAME_CONTENT in source:
                raw_content = source[DC_NODE_EXT_FEATURE_NAME_CONTENT]
            if 'extractions' in source:
                extractions = source['extractions']

                if DC_NODE_EXT_FEATURE_NAME_POSTTIME in extractions:
                    posttime = es_content_loader(extractions[DC_NODE_EXT_FEATURE_NAME_POSTTIME]['results'])
                if DC_NODE_EXT_FEATURE_NAME_CITY in extractions:
                    city = es_content_loader(extractions[DC_NODE_EXT_FEATURE_NAME_CITY]['results'])
                if DC_NODE_EXT_FEATURE_NAME_TEXT in extractions:
                    text = es_content_loader(extractions[DC_NODE_EXT_FEATURE_NAME_TEXT]['results'])
                if DC_NODE_EXT_FEATURE_NAME_REGION in extractions:
                    region = es_content_loader(extractions[DC_NODE_EXT_FEATURE_NAME_REGION]['results'])
                if DC_NODE_EXT_FEATURE_NAME_TITLE in extractions:
                    title = es_content_loader(extractions[DC_NODE_EXT_FEATURE_NAME_TITLE]['results'])
                if DC_NODE_EXT_FEATURE_NAME_USERLOCATION in extractions:
                    userlocation = es_content_loader(extractions[DC_NODE_EXT_FEATURE_NAME_USERLOCATION]['results'])
                if DC_NODE_EXT_FEATURE_NAME_PHONENUMBER in extractions:
                    phonenumber = es_content_loader(extractions[DC_NODE_EXT_FEATURE_NAME_PHONENUMBER]['results'])
                if DC_NODE_EXT_FEATURE_NAME_SID in extractions:
                    sid = es_content_loader(extractions[DC_NODE_EXT_FEATURE_NAME_SID]['results'])
                if DC_NODE_EXT_FEATURE_NAME_OTHERADS in extractions:
                    otherads = es_content_loader(extractions[DC_NODE_EXT_FEATURE_NAME_OTHERADS]['results'])
                if DC_NODE_EXT_FEATURE_NAME_AGE in extractions:
                    age = es_content_loader(extractions[DC_NODE_EXT_FEATURE_NAME_AGE]['results'])

                new_node = Node( \
                    raw_content, \
                    doc_id=doc_id, \
                    posttime=posttime, \
                    city=city, \
                    text=text, \
                    region=region, \
                    title=title, \
                    userlocation=userlocation, \
                    phonenumber=phonenumber, \
                    sid=sid, \
                    otherads=otherads, \
                    age=age)

                dataset.append(new_node)
                # break
        return dataset

    def __load_data_csv(path):
        import csv
        dataset = []
        with open(path, 'rb') as csvfile:
        # with codecs.open(path, 'r', 'utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)
            for row in reader:
                label =row[0]
                content = row[1].decode('utf-8', 'ignore').encode('ascii', 'ignore')

                new_node = Node( \
                    content, \
                    label=label, \
                    doc_id=row[header.index(DC_NODE_EXT_FEATURE_NAME_DOCID)], \
                    posttime=row[header.index(DC_NODE_EXT_FEATURE_NAME_POSTTIME)], \
                    city=row[header.index(DC_NODE_EXT_FEATURE_NAME_CITY)], \
                    text=row[header.index(DC_NODE_EXT_FEATURE_NAME_TEXT)], \
                    region=row[header.index(DC_NODE_EXT_FEATURE_NAME_REGION)], \
                    title=row[header.index(DC_NODE_EXT_FEATURE_NAME_TITLE)], \
                    userlocation=row[header.index(DC_NODE_EXT_FEATURE_NAME_USERLOCATION)], \
                    phonenumber=row[header.index(DC_NODE_EXT_FEATURE_NAME_PHONENUMBER)], \
                    sid=row[header.index(DC_NODE_EXT_FEATURE_NAME_SID)], \
                    otherads=row[header.index(DC_NODE_EXT_FEATURE_NAME_OTHERADS)], \
                    age=row[header.index(DC_NODE_EXT_FEATURE_NAME_AGE)])

                dataset.append(new_node)
                # break
        return dataset

    __loader_load_data_funcs = {
        DC_DATA_FILE_FORMAT_SEQUENCE: __load_data_sequence,
        DC_DATA_FILE_FORMAT_JSONLINES: __load_data_jsonlines,
        DC_DATA_FILE_FORMAT_JSON: __load_data_json,
        DC_DATA_FILE_FORMAT_CSV: __load_data_csv
    }

    @staticmethod
    def load_data(path, format=DC_DATA_FILE_FORMAT_JSONLINES):
        """
        basic interface to load data
        
        """
        return Loader.__loader_load_data_funcs[format](path)

    #################################################
    # Generate Data Functions
    #################################################
    
    def __generate_data_jsonlines(dataset, path):
        pass

    def __generate_data_json(dataset, path):
        pass

    def __generate_data_csv(dataset, path):
        if not dataset:
            return
        with open(path, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=[DC_LABEL_NAME,DC_NODE_EXT_FEATURE_NAME_CONTENT]+DC_NODE_EXT_FEATURES)
            writer.writeheader()
            for data in dataset:
                try:
                    content = data._content
                    if not content:
                        continue
                    content = content.replace('\n', ' ').replace('\r', ' ')
                    # print '1111111111111111'
                    content = unicode(content, errors='ignore')
                    # print content#.decode('utf-8', 'ignore')
                    # print '2222222222222222'
                    # content = ''
                    data = {k:v.encode('utf-8').replace('\n', ' ').replace('\r', ' ') for (k, v) in data._attrs.iteritems() if v}
                    data.setdefault(DC_LABEL_NAME, -1)
                    data.setdefault(DC_NODE_EXT_FEATURE_NAME_CONTENT, content)
                    # print '3333333333333333'
                    # print '#'*100
                    writer.writerow(data)
                    # print '4444444444444444'
                except Exception as e:
                    # print content
                    print e
                    raise Exception('error in __generate_data_csv')
                    
                
                # break

    __loader_generate_data_funcs = {
        DC_DATA_FILE_FORMAT_JSONLINES: __generate_data_jsonlines,
        DC_DATA_FILE_FORMAT_JSON: __generate_data_json,
        DC_DATA_FILE_FORMAT_CSV: __generate_data_csv
    }

    @staticmethod
    def generate_data(dataset, path, format=DC_DATA_FILE_FORMAT_JSONLINES):
        Loader.__loader_generate_data_funcs[format](dataset, path)

    #################################################
    # Main Load Data Functions
    #################################################
    
    @staticmethod
    def load_training_data(filepath=DC_DEFAULT_TRAINING_DATA_FILEPATH):
        return Loader.load_data(filepath, format=DC_DATA_FILE_FORMAT_CSV)

    @staticmethod
    def load_testing_data(filepath=DC_DEFAULT_TESTING_DATA_FILEPATH):
        return Loader.load_data(filepath, format=DC_DATA_FILE_FORMAT_CSV)

    @staticmethod
    def load_dig_data(filepath=DC_DEFAULT_DIG_WEBPAGE_DATA_FILEPATH, \
                    output_filepath=DC_DEFAULT_TRAINING_DATA_FILEPATH, \
                    format=DC_DATA_FILE_FORMAT_CSV):
        data = Loader.load_data(filepath, format=DC_DATA_FILE_FORMAT_JSON)
        if output_filepath:
            Loader.generate_data(data, output_filepath, format=format)
        return data

    @staticmethod
    def load_memex_data(filepath=DC_DEFAULT_DIG_WEBPAGE_DATA_FILEPATH, \
                    output_filepath=DC_DEFAULT_TRAINING_DATA_FILEPATH, \
                    format=DC_DATA_FILE_FORMAT_CSV):
        data = Loader.load_data(filepath, format=DC_DATA_FILE_FORMAT_SEQUENCE)

    @staticmethod
    def load_vectors(nodes):
        vectors = []
        for node in nodes:
            vector = node.generate_vector()
            vectors.append(vector)
        return vectors

if __name__ == '__main__':

    # Loader.load_training_data()
    # Loader.load_dig_data(filepath=DC_DEFAULT_DIG_WEBPAGE_DATA_FILEPATH, output_filepath=DC_DEFAULT_TRAINING_DATA_FILEPATH)
    Loader.load_memex_data(filepath='/Volumes/Expansion/2016_memex/readability/part-00000')




