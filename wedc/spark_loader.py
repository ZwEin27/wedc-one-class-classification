# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-06-20 10:55:39
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-09-02 15:43:52


"""
919114 lines of data in total
184194 contains massage

"""

"""

spark-submit \
--conf "spark.yarn.executor.memoryOverhead=8192" \
--conf "spark.rdd.compress=true" \
--conf "spark.shuffle.compress=true" \
--driver-memory 6g \
--executor-memory 6g  --executor-cores 4  --num-executors 20 \
/Users/ZwEin/job_works/StudentWork_USC-ISI/projects/wedc-one-class-classification/wedc/spark_loader.py \
--input_file /Users/ZwEin/job_works/StudentWork_USC-ISI/dataset/readability \
--output_dir /Volumes/Expansion/2016_memex/readability_output



--files_dir /Users/ZwEin/job_works/StudentWork_USC-ISI/projects/WEDC/spark_dependencies/python_files

"""

import json
import sys
import os
import re
import string
import argparse
from pyspark import SparkContext, SparkConf, SparkFiles
from digSparkUtil.fileUtil import FileUtil

re_word = re.compile(r'[a-zA-Z0-9]+')

DC_DEFAULT_KEYWORDS_MASSAGE = [
    'massages',
    'touch',
    'sexy',
    'charm',
    'energy',
    'bodyrub',
    'lovely',
    'full',
    'relaxation',
    'session',
    'beautiful',
    'teasing',
    'addition',
    'sweet',
    'spirit',
    'unique',
    'pleasure',
    'tantra',
    'erotic',
    'prostate',
    'therapeutic',
    'tantric',
    'oil',
    'sensitive',
    'sensual',
    'relaxing',
    'therapist',
    'therapy',
    'body',
    'nude',
    'classic',
    'jessie',
    'massage',
    'spa',
    'thai',
    'kamasutra',
    'aromatherapy'
]

DC_DEFAULT_KEYWORDS_ESCORT = [
    'click',
    'tel',
    'sorry',
    'call',
    'incall',
    'outcall',
    'hh',
    'hr',
    'quick',
    'quickie',
    'hott',
    'legged',
    'busty',
    'male',
    'playboy',
    'gigolo',
    'handsome',
    'hunk',
    'ts',
    'tv',
    'transvestite',
    'tranny',
    'tgirl',
    'shemale',
    'she-male',
    'transsexual',
    'transexual',
    'ladyboy'
]

DC_DEFAULT_KEYWORDS_JOB_ADS = [
    'employee',
    'manager',
    'OSHA',
    'license',
    'business',
    'technician',
    'certified',
    'degree',
    'salary',
    'retail',
    '401k',
    'insurance'
]


DC_DEFAULT_KEYWORDS = {
    'massage': DC_DEFAULT_KEYWORDS_MASSAGE,
    'escort': DC_DEFAULT_KEYWORDS_ESCORT,
    'job_ads': DC_DEFAULT_KEYWORDS_JOB_ADS,
}



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


def run(sc, input_file, output_dir, cateory='massage'):

    def map_load_data(keywords):
        def _map_load_data(data):
            key, json_obj = data
            # ans = []
            if 'readability_text' in json_obj:
                desc = extract_content(json_obj['readability_text'])
                tokens = re_word.findall(desc)
                for keyword in keywords:
                    if keyword in tokens:
                        # ans.append(json_obj)
                        return (str(key), json_obj)
            return (str(key), '')
        return _map_load_data


        # return (str(key), ' '.join(ans))

    # for file_path in os.listdir(files_dir):
    #     if file_path[0] != '.':
    #         sc.addFile(os.path.join(files_dir, file_path))

    # print os.listdir(SparkFiles.getRootDirectory())
    # print os.listdir(os.path.join(SparkFiles.getRootDirectory(), 'python_files.zip'))
    # if os.path.isfile(SparkFiles.get(os.path.join('python_files.zip', 'en', 'lexnames'))):
    #     print 'exist'

    keywords = DC_DEFAULT_KEYWORDS[cateory]
    
    rdd_original = load_jsonlines(sc, input_file)
    rdd_content = rdd_original.map(map_load_data('massage'))
    rdd_content = rdd_content.values()

    def my_method(line):
        return [line] if not isinstance(line, basestring) else []
    rdd_content = rdd_content.flatMap(lambda line: my_method(line))
    # print rdd_original.collect()[0]
    # print rdd_content.count()

    # rdd.saveAsTextFile(output_dir)
    # save_jsonlines(sc, rdd, output_dir, file_format='sequence', data_type='json')
    
    rdd_content = rdd_content.coalesce(1)   # output single
    rdd_content.saveAsTextFile(output_dir)
    # save_jsonlines(sc, rdd_content, output_dir, file_format='text', data_type='json')


    # dataset = rdd_content.collect()
    # with open(os.path.join(output_dir, cateory), 'wb') as file_handler:
    #     for data in dataset:
    #         file_handler.write(json.dumps(data, indent=4))


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-i','--input_file', required=True)
    arg_parser.add_argument('-o','--output_dir')#, required=True)

    args = arg_parser.parse_args()

    spark_config = SparkConf().setAppName('WEDC')
    sc = SparkContext(conf=spark_config)

    run(sc, args.input_file, args.output_dir)