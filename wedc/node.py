# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-08 11:46:11
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-09 15:03:30


from vendor.crf_tokenizer import CrfTokenizer
from common import cleaner
from seed import seeds

DC_NODE_FEATURES = [ 
    'ext_url'
] + sorted(seeds)

class Node(object):

    def __init__(self, content, sid=None, label=None):
        self._content = content
        self._sid = sid
        self._label = label
        self._seeds = seeds
        self._features = self.load_features(content)
        self._vector = self.generate_vector()

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
        ans = {}
        content = self.clean(content)
        seed_words = seeds.keys()
        seeds_size = len(seed_words)
        # seed_words.sort()
        tokens = content.split(' ')
        for i in range(seeds_size):
            if seed_words[i] in tokens:
                ans.setdefault(seed_words[i], str(1.0 * float(seeds[seed_words[i]])))
        return ans

    def load_ext_features(self, content):
        return {}

    def load_features(self, content):
        return dict(self.load_seed_features(content).items() + self.load_ext_features(content).items())

    #################################################
    # Generate Vector
    #################################################
    
    def generate_vector(self):
        if not self._features:
            return []
        features = DC_NODE_FEATURES
        feature_size = len(features)
        vector = ['0'] * feature_size
        for i in range(feature_size):
            if features[i] in self._features:
                vector[i] = str(1.0 * float(self._features[features[i]]))
        return ' '.join(vector)
               







