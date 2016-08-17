# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-08 11:46:11
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-17 14:03:00


from vendor.crf_tokenizer import CrfTokenizer
from common import cleaner
from seed import seeds

DC_NODE_EXT_FEATURE_NAME_CONTENT = 'extracted_text'    # extracted_text
DC_NODE_EXT_FEATURE_NAME_POSTTIME = 'posttime'
DC_NODE_EXT_FEATURE_NAME_CITY = 'city'
DC_NODE_EXT_FEATURE_NAME_TEXT = 'text'
DC_NODE_EXT_FEATURE_NAME_REGION = 'region'
DC_NODE_EXT_FEATURE_NAME_TITLE = 'title'
DC_NODE_EXT_FEATURE_NAME_USERLOCATION = 'userlocation'
DC_NODE_EXT_FEATURE_NAME_PHONENUMBER = 'phonenumber'
DC_NODE_EXT_FEATURE_NAME_SID = 'sid'
DC_NODE_EXT_FEATURE_NAME_OTHERADS = 'otherads'
DC_NODE_EXT_FEATURE_NAME_AGE = 'age'
DC_NODE_EXT_FEATURE_NAME_DOCID = 'doc_id'

DC_NODE_EXT_FEATURE_NAME_EMAIL = 'email'
DC_NODE_EXT_FEATURE_NAME_DRUG_USE = 'drug_use'
DC_NODE_EXT_FEATURE_NAME_EMAIL = 'email'
DC_NODE_EXT_FEATURE_NAME_EMAIL = 'email'
DC_NODE_EXT_FEATURE_NAME_EMAIL = 'email'
DC_NODE_EXT_FEATURE_NAME_EMAIL = 'email'
DC_NODE_EXT_FEATURE_NAME_EMAIL = 'email'
DC_NODE_EXT_FEATURE_NAME_EMAIL = 'email'

location=location, \
                email=email, \
                drug_use=drug_use, \
                price=price, \
                price_per_hour=price_per_hour, \
                business_type=business_type, \
                url=url, \
                services=services, \
                gender=gender, \
                phone=phone, \
                age=age)

DC_NODE_EXT_FEATURES = [
    DC_NODE_EXT_FEATURE_NAME_CITY,
    DC_NODE_EXT_FEATURE_NAME_TEXT,
    DC_NODE_EXT_FEATURE_NAME_REGION,
    DC_NODE_EXT_FEATURE_NAME_TITLE,
    DC_NODE_EXT_FEATURE_NAME_USERLOCATION,
    DC_NODE_EXT_FEATURE_NAME_PHONENUMBER,
    DC_NODE_EXT_FEATURE_NAME_SID,
    DC_NODE_EXT_FEATURE_NAME_OTHERADS,
    DC_NODE_EXT_FEATURE_NAME_AGE,
    DC_NODE_EXT_FEATURE_NAME_DOCID
]

DC_NODE_FEATURES = DC_NODE_EXT_FEATURES + sorted(seeds)

class Node(object):

    def __init__(self, content, sid=None, label=None, **attrs):
        self._content = content.encode('utf-8') if content else None
        self._sid = sid
        self._label = label
        self._seeds = seeds
        self._attrs = attrs
        self._features = self.load_features(content)
        self._vector = self.generate_vector()

        # print self._features        
        # for attr in sorted(self.attrs.keys()):
        #     print attr, ':', attrs[attr]

    #################################################
    # Clean Content 
    #################################################

    def clean(self, text):
        if not text:
            return ''
        # text = text.encode('ascii', 'ignore')
        text = cleaner.clean_text(text)
        text = text.encode('utf-8')
        # text = unicode(text, 'utf-8')
        # try:
        #     text = text.encode('ascii', 'ignore')
        # except:
        #     text = text.decode('utf-8', 'ignore')
        t = CrfTokenizer()
        t.setRecognizeHtmlEntities(True)
        t.setRecognizeHtmlTags(True)
        t.setSkipHtmlTags(True)
        tokens = t.tokenize(text)

        tokens = [cleaner.clean_token(token) for token in tokens]
        tokens = [_ for _ in set(tokens) if _]

        try:

            str(' '.join(tokens))
        except:
            print text.encode('utf-8')
            print tokens

        return str(' '.join(tokens)).decode('utf-8', 'ignore').encode('ascii', 'ignore')

    #################################################
    # Load Feature
    #################################################
    
    def load_seed_features(self, content):
        if not content:
            return {}
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
        if self._attrs:
            # print self._attrs
            ans = {}
            for (attr_name, attr_value) in self._attrs.iteritems():
                # print attr_value, attr_value
                if attr_value and attr_value != '':
                    ans.setdefault(attr_name, 1.)
            return ans
        return {}

    def load_features(self, content):
        return dict(self.load_seed_features(content).items() + self.load_ext_features(content).items())

    #################################################
    # Transformat
    #################################################
    
    def generate_vector(self):
        features = DC_NODE_FEATURES
        feature_size = len(features)
        if not self._features:
            return ' '.join(['0'] * feature_size)
        vector = ['0'] * feature_size
        for i in range(feature_size):
            if features[i] in self._features:
                vector[i] = str(1.0 * float(self._features[features[i]]))
        return ' '.join(vector)

               







