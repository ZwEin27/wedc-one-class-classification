# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-10 13:53:23
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-10 14:32:13


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

DC_STREAMER_DEFAULT_KEYWORDS = [
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
    'ladyboy',
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
    'insurance',
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
    'jacuzzi'
]

######################################################################
#   Regular Expression
######################################################################

re_tokenize = re.compile(r'[\s!\"#\$%&\'\(\)\*\+,\-\./:;<=>\?@\[\\\]\^_`{|}~]')

######################################################################
#   Query
######################################################################

sites_query = {
    "aggs": {
        "by-posttime": {
            "filter": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "version": "2.0"
                            }
                        },
                        {
                            "exists": {
                                "field": "extractions.text"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "posttime": {
                    "terms": {
                        "field": "extractions.text.attribs.target",
                        "size": 1000
                    }
                }
            }
        }
    },
    "size": 0
}

search_query = { 
    "query": {
        "filtered": {
          "filter": {
            "exists": {
              "field": "extractions.text"
            }
          },
          "query": {
            "match": {
                "extractions.text.results": "massage"
            }
          }
        }
    },
    "_source": [ 
      "raw_content", 
      "extractions.posttime.results",
      "extractions.city.results",
      "extractions.text.results",
      "extractions.region.results",
      "extractions.title.results",
      "extractions.userlocation.results",
      "extractions.phonenumber.results",
      "extractions.sid.results",
      "extractions.otherads.results",
      "extractions.age.results",
      "doc_id"
    ],
    "size": 1
}



######################################################################
#   Main Class
######################################################################

class Streamer(object):

    def __init__(self, token):
        # self.username, self.password = token.split(':')
        self.cdr = 'https://' + token + '@cdr-es.istresearch.com:9200/memex-domains'
        self.es = Elasticsearch([self.cdr])

    def load_sites(self):
        buckets = self.es.search(index='escorts', body=sites_query)['aggregations']['by-posttime']['posttime']['buckets']
        sites = map(lambda x: x['key'], buckets)
        return sites

    def load_data(self, site_name, keyword):
        # load data for specifc site name
        try:
            search_query['query']['filtered']['filter']['bool']['must'][0]['term']['extractions.text.attribs.target'] = site_name
            search_query['query']['filtered']['query']['match']['extractions.text.results'] = keyword
            buckets = self.es.search(index='escorts', body=search_query)['hits']['hits']
        except Exception as e:
            raise Exception('site_name is incorrect')

        # load fetched data
        ans = []
        for bucket in buckets:
            try:
                text = bucket['_source']['extractions']['text']['results']
                if not isinstance(text, basestring):
                    text = ' '.join(text)
            except Exception as e:
                continue
            else:
                ans.append(text)
        return ans

    def dedup_data(self, data_lines):

        def clean(data):
            return ' '.join(sorted([_.strip() for _ in re_tokenize.split(data) if _.strip() != '']))

        def hash(data):
            return hashlib.sha224(clean(data).encode('ascii', 'ignore')).hexdigest()#+hashlib.sha256(data).hexdigest()+hashlib.md5(data).hexdigest()

        # clean
        # data_lines = [clean(_) for _ in data_lines]

        # dedup
        dedup = {}
        for data in data_lines:
            dedup[hash(data)] = data
        data_lines = dedup.values()

        return data_lines

    def generate(self, output_path=None, keywords=['massage','escort','street','st','avenue','rd','boulevard','parkway','pkwy'], num_data=200):
        ans = {}
        sites = self.load_sites()
        for site_name in sites:
            data = []
            for keyword in keywords:
                ans.setdefault(keyword, [])
                ans[keyword] += self.load_data(site_name, keyword)
                # data += self.load_data(site_name, keyword)
            # data = data[:num_data]
            # data = self.dedup_data(data)[:num_data]
            # ans += data
        ans = {k:self.dedup_data(v) for (k, v) in ans.iteritems()}

        if output_path:
            file_handler = open(output_path, 'wb')
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

    dg = DIGESDG(args.token)  
       
    print dg.generate(keywords=keywords, num_data=num_data, output_path=args.output_path)