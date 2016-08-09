# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-09 13:52:35
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-09 13:55:11


import re
import string

def hasNumbers(inputString):
    # return any(char.isdigit() for char in inputString)
    return bool(re.search(r'\d', inputString))


def hasUnicode(s):
    if isinstance(s, unicode):
        return True
    return False

def hasSpecial(s):
    # unicode_char = u'aa\u2764\ufe0f\u0455\u03c9\u0454\u0454\u0442\u0454\u0455\u0442'
    # unicode_char = u'ss'
    
    encoded = s.encode('utf-8')
    reg = re.compile("^[A-Za-z"+string.punctuation+"]+$")
    if reg.search(encoded):
        return False
    return True

def hasPunctuation(s):
    reg = re.compile("["+string.punctuation+"]+")
    if reg.search(s):
        return True
    return False

def hasHTMLTag(s):
    reg = re.compile("<\w+>")
    if reg.search(s):
        return True
    return False


def whatisthis(s):
    if isinstance(s, str):
        print "ordinary string"
    elif isinstance(s, unicode):
        print "unicode string"
    else:
        print "not a string"


# print hasUnicode(u'\u2113')

# print string.punctuation
# s = '<p>Source: <a rel="nofollow" target="_blank" href="http://www.jobs2careers.com/click.php?id=1834227799.96&amp;job_loc=Santa+Maria%2CCA">http://www.jobs2careers.com/click.php?id=1834227799.96&amp;job_loc=Santa+Maria%2CCA</a></p>'

# print hasHTMLTag(s)


# print hasNumbers('live')