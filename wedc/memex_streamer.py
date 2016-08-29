# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-10 13:53:23
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-29 15:07:06


import urllib3
import re
import sys
from elasticsearch import Elasticsearch
import json
import getopt
import hashlib

urllib3.disable_warnings()


######################################################################
#   Constant
######################################################################


DC_STREAMER_DEFAULT_KEYWORDS_MASSAGE = [
    'spa',
    'table',
    'shower',
    'nuru',
    'slide',
    'therapy',
    'therapist',
    'bodyrub',
    'sauna',
    'gel',
    'shiatsu',
    'jacuzzi',
    'massage'
]

DC_STREAMER_DEFAULT_KEYWORDS_ESCORT = [
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

DC_STREAMER_DEFAULT_KEYWORDS_JOB_ADS = [
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



DC_STREAMER_DEFAULT_KEYWORDS = DC_STREAMER_DEFAULT_KEYWORDS_ESCORT



######################################################################
#   Regular Expression
######################################################################

re_tokenize = re.compile(r'[\s!\"#\$%&\'\(\)\*\+,\-\./:;<=>\?@\[\\\]\^_`{|}~]')

######################################################################
#   Query
######################################################################

search_query = { 
    "size": 30
}



######################################################################
#   Main Class
######################################################################

class Streamer(object):

    def __init__(self, token):
        self.cdr = 'https://' + token + '@esc.memexproxy.com'
        self.es = Elasticsearch([self.cdr])

    def load_data(self, keyword):
        # load data for specifc site name
        try:
            # search_query['query']['filtered']['query']['match']['extractions.text.results'] = keyword
            buckets = self.es.search(index='dig-extractions-july-16', body=search_query)['hits']['hits']
        except Exception as e: 
            return []
            raise Exception('load data error')
        # print buckets
        # load fetched data
        # ans = []
        # for bucket in buckets:
        #     try:
        #         text = json.dumps(bucket, sort_keys=True, indent=4)
        #     except Exception as e:
        #         continue
        #     else:
        #         ans.append(text)
        return buckets

    def dedup_data(self, data_lines):

        def clean(data):
            return ' '.join(sorted([_.strip() for _ in re_tokenize.split(data) if _.strip() != '']))

        def hash(data):
            return hashlib.sha224(clean(data).encode('ascii', 'ignore')).hexdigest()

        dedup = {}
        for data in data_lines:
            data = json.loads(data)
            content = data['_source']['extractions']['text']['results']
            if not isinstance(content, basestring):
                content = ' '.join(content)
            dedup[hash(content)] = data
        data_lines = dedup.values()

        return data_lines

    def generate(self, output_path=None, keywords=DC_STREAMER_DEFAULT_KEYWORDS, num_data=20):
        ans = []
        for keyword in keywords:
            print 'keyword:', keyword
            ans += self.load_data(keyword)
        # ans = self.dedup_data(ans)
        if output_path:
            file_handler = open(output_path, 'w')
            file_handler.write(json.dumps(ans, sort_keys=True, indent=4))
            file_handler.close()

        return ans


if __name__ == '__main__':
    import sys
    import argparse

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-t','--token', required=True)
    arg_parser.add_argument('-k','--keywords', required=False)
    arg_parser.add_argument('-n','--num_data', required=False)
    arg_parser.add_argument('-o','--output_path', required=False)

    args = arg_parser.parse_args()

    streamer = Streamer(args.token)

    keywords = args.keywords if args.keywords else DC_STREAMER_DEFAULT_KEYWORDS
    num_data = args.num_data if args.num_data else 10
       
    streamer.generate(keywords=keywords, num_data=num_data, output_path=args.output_path)