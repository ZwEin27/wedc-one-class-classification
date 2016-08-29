# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-06-20 10:55:39
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-29 13:40:54

"""

spark-submit \
--conf "spark.yarn.executor.memoryOverhead=8192" \
--conf "spark.rdd.compress=true" \
--conf "spark.shuffle.compress=true" \
--driver-memory 6g \
--executor-memory 6g  --executor-cores 4  --num-executors 20 \
/Users/ZwEin/job_works/StudentWork_USC-ISI/projects/wedc-one-class-classification/wedc/spark_loader.py \
--input_file /Volumes/Expansion/2016_memex/readability \
--output_dir /Volumes/Expansion/2016_memex/readability_output



--files_dir /Users/ZwEin/job_works/StudentWork_USC-ISI/projects/WEDC/spark_dependencies/python_files

"""

import json
import sys
import os
import argparse
from pyspark import SparkContext, SparkConf, SparkFiles
from digSparkUtil.fileUtil import FileUtil


def load_jsonlines(sc, input, file_format='sequence', data_type='json', separator='\t'):
    fUtil = FileUtil(sc)
    rdd = fUtil.load_file(input, file_format=file_format, data_type=data_type, separator=separator)
    return rdd

def save_jsonlines(sc, rdd, output_dir, file_format='sequence', data_type='json', separator='\t'):
    fUtil = FileUtil(sc)
    fUtil.save_file(rdd, output_dir, file_format=file_format, data_type=data_type, separator=separator)

def load_labelled_data_file(path):
    labelled_data = []
    with open(path, 'rb') as f:
        for line in f.readlines():
            line = line.strip().split('\t')
            labelled_data.append([line[0], line[1]])
    return labelled_data


def extract_content(raw):
    if not raw:
        return ''
    content = []
    if isinstance(raw, basestring):
        content.append(raw)
    else:
        content = raw
    return ' '.join(content)


def run(sc, input_file, output_dir):

    def map_load_data(data):
        key, json_obj = data
        text_list = []
        if 'description' in json_obj:
            desc = extract_content(json_obj['description'])
            text_list.append(desc)
        if 'name' in json_obj:
            name = extract_content(json_obj['name'])
            text_list.append(name)
        return (str(key), ' '.join(text_list))

    # for file_path in os.listdir(files_dir):
    #     if file_path[0] != '.':
    #         sc.addFile(os.path.join(files_dir, file_path))
    

    # print os.listdir(SparkFiles.getRootDirectory())
    # print os.listdir(os.path.join(SparkFiles.getRootDirectory(), 'python_files.zip'))
    # if os.path.isfile(SparkFiles.get(os.path.join('python_files.zip', 'en', 'lexnames'))):
    #     print 'exist'
    

    rdd_original = load_jsonlines(sc, input_file)
    # rdd_content = rdd_original.map(map_load_data)
 

    # rdd.saveAsTextFile(output_dir)
    # save_jsonlines(sc, rdd, output_dir, file_format='sequence', data_type='json')
    

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-i','--input_file', required=True)
    arg_parser.add_argument('-o','--output_dir')#, required=True)

    args = arg_parser.parse_args()

    spark_config = SparkConf().setAppName('WEDC')
    sc = SparkContext(conf=spark_config)

    run(sc, args.input_file, args.output_dir)