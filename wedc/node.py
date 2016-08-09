# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-08 11:46:11
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-09 13:38:02


class Node(object):

    def __init__(self, content, sid=None, label=None):
        self._content = content
        self._sid = sid
        self._label = label

        self.features = self.load_features(content)

    #################################################
    # Feature Loader
    #################################################
    
    def load_seed_features(self, content):
        return {}

    def load_ext_features(self, content):
        return {}

    def load_features(self, content):
        return dict(self.load_seed_features(content).items() + self.load_ext_features(content).items())


