# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-08 11:46:11
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-09 14:12:42


from vendor.crf_tokenizer import CrfTokenizer
from common import cleaner

class Node(object):

    def __init__(self, content, sid=None, label=None):
        self._content = content
        self._sid = sid
        self._label = label

        self.features = self.load_features(content)

    #################################################
    # Clean Content 
    #################################################

    def clean(self, text):
        # text = text.encode('ascii', 'ignore')
        text = cleaner.clean_text(text)
        t = CrfTokenizer()
        t.setRecognizeHtmlEntities(True)
        t.setRecognizeHtmlTags(True)
        t.setSkipHtmlTags(True)
        tokens = t.tokenize(text)
        tokens = [cleaner.clean_token(token) for token in tokens]

        tokens = [_ for _ in tokens if _]
        return str(' '.join(set(tokens)))


    #################################################
    # Load Feature
    #################################################
    
    def load_seed_features(self, content):
        content = self.clean(content)
        print content
        return {}

    def load_ext_features(self, content):
        return {}

    def load_features(self, content):
        return dict(self.load_seed_features(content).items() + self.load_ext_features(content).items())


