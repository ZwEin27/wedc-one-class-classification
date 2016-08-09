# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-09 13:51:02
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-09 13:54:59


"""
Credits:
NodeBox::Linguistics: https://www.nodebox.net/code/index.php/Linguistics#verb_conjugation
https://www.nodebox.net/code/index.php/Linguistics#verb_conjugation
"""
import sys
import os

# sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "vendor"))
# import en

import inflection

be_words = ['am', 'is', 'are', 'was', 'were', 'been']

def stemming(word):

    # if word in be_words:
    #     return 'be'

    # try:
    #     if en.is_noun(word):
    #         word = en.noun.singular(word)
    #     elif en.is_adjective(word) or en.is_adverb(word):
    #         return word
    #     else:
    #         word = en.verb.present(word)
    # except Exception:
    #     pass
    # return en.spelling.correct(word)
    
    word = inflection.singularize(word)
    return word


    

if __name__ == "__main__":

    word = 'words'
    word = inflection.singularize(word)
    print word


