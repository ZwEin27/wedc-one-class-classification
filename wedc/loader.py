# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-08 11:46:11
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-11 13:55:52


import os
import csv
import json
import codecs
from node import *

DC_DATA_FILE_FORMAT_JSONLINES = 'jsonlines'
DC_DATA_FILE_FORMAT_JSON = 'json'
DC_DATA_FILE_FORMAT_CSV = 'csv'

DC_DATA_FILE_FORMATS = [
    DC_DATA_FILE_FORMAT_JSONLINES,
    DC_DATA_FILE_FORMAT_JSON,
    DC_DATA_FILE_FORMAT_CSV
]


DC_DEFAULT_DIG_WEBPAGE_DATA_FILEPATH = os.path.join(os.path.dirname(__file__), 'res', 'webpages.json')
DC_DEFAULT_TRAINING_DATA_FILEPATH = os.path.join(os.path.dirname(__file__), 'res', 'training_data.csv')
DC_DEFAULT_TESTING_DATA_FILEPATH = os.path.join(os.path.dirname(__file__), 'res', 'testing_data.csv')


class Loader(object):

    #################################################
    # Basic Load Data Functions
    #################################################

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
        json_objs = json.load(codecs.open(path, 'r', 'utf-8'))
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
                break
        return dataset

    def __load_data_csv(path):
        import csv
        dataset = []
        with open(path, 'rb') as csvfile:
        # with codecs.open(path, 'r', 'utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                new_node = Node(row[1].decode('utf-8', 'ignore').encode('ascii', 'ignore'), label=row[0], hasPhone=True)
                dataset.append(new_node)
                # break
        return dataset

    __loader_data_funcs = {
        DC_DATA_FILE_FORMAT_JSONLINES: __load_data_jsonlines,
        DC_DATA_FILE_FORMAT_JSON: __load_data_json,
        DC_DATA_FILE_FORMAT_CSV: __load_data_csv
    }

    @staticmethod
    def load_data(path, format=DC_DATA_FILE_FORMAT_JSONLINES):
        """
        basic interface to load data
        
        """
        return Loader.__loader_data_funcs[format](path)

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
    def load_dig_data(filepath=DC_DEFAULT_DIG_WEBPAGE_DATA_FILEPATH):
        return Loader.load_data(filepath, format=DC_DATA_FILE_FORMAT_JSON)

    @staticmethod
    def load_vectors(nodes):
        vectors = []
        for node in nodes:
            vector = node.generate_vector()
            vectors.append(vector)
        return vectors


if __name__ == '__main__':

    # Loader.load_training_data()
    Loader.load_dig_data()




