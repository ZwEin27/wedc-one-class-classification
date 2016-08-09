# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-09 13:50:35
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-09 14:13:21


# from common import stopword
# from common import hasher
# from common import stemmer
# from common import string
import re
import stopword
import hasher
import stemmer
import string

stopset = stopword.get_stopword_set()

token_mapping = {
    'hrs': 'hour',
    'luv': 'love',
    'lookin': 'look',
    'californias': 'california',
    'pleasured': 'pleasure',
    'neww': 'new',
    'newly': 'new',
    'newest': 'new',
    'renew': 'new',
    'sweety': 'sweet',
    'sweetness': 'sweet',
    'sweetheart': 'sweet',
    'sweetie': 'sweet',
    'gon': 'go',
    'wanna': 'want',
    'incal': 'incall',
    'incalls': 'incall',
    'incallz': 'incall',
    'outcal': 'outcall',
    'outcalls': 'outcall',
    'incallz': 'outcall',
    'inn': 'in'
}

# seed_words = seed_word.load_seed_words()


############################################################
#   Posts
############################################################

def remove_dups(posts, mapping_path=None):
    hs = set()
    size = len(posts)
    no_dups = []
    new_pid = 0
    mapping_file = None
    if mapping_path:
        mapping_file = open(mapping_path, 'w')

    for pid in xrange(1, size+1):
        post = posts[pid-1] # pid start from 1
        if not post or post.strip() == '':
            continue
        checksum = hasher.checksum(content)
        if checksum not in hs:
            hs.add(checksum)
            no_dups.append(post)
            new_pid += 1
            if mapping_file:
                mapping_file.write(str(new_pid)+'\t'+str(pid)+'\n')
    if mapping_file:
        mapping_file.close()
    return no_dups


############################################################
#   Text
############################################################

def clean_text(text):
    # convert html code
    text = unescape(text)
    # text = text.encode('ascii', 'ignore')

    # remove tags
    text = re.sub(r'<[^>]+>', ' ', text)

    # remove url
    text = re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', text)
    # text = re.sub(r'^[a-z0-9\-\.]+\.(com|org|net|me|cn|biz|us)$', '', text)

    # remove space
    text = re.sub(r'([\t\n\r]|\\+)', ' ', text)

    # remove ' as well as followings
    text = re.sub(r"'\w+", '', text)

    # remove punctuation
    text = re.sub(r'[' + string.punctuation +']+', ' ', text)
    # text = text.translate(string.maketrans('', ''), string.punctuation)
    
    return ' '.join(text.split())  

############################################################
#   Sentence
############################################################

# Leave Blank

############################################################
#   Token
############################################################

def mars2norm(token):
    if token.lower() in token_mapping:
        return token_mapping[token.lower()]
    return token

def clean_token(token):

    # if token.lower() in seed_words:
    #     return token.lower()

    if re.search(r'(\d+[k$]+[/(hr|hour)]*|401[\w\d]*)', token.lower()):
        return '401k'

    if re.search(r'girl', token.lower()):
        return 'girl'

    if re.search(r'job', token.lower()):
        return 'job'

    if re.search(r'day', token.lower()):
        return 'day'

    if re.search(r'night', token.lower()):
        return 'night'

    if re.search(r'^[xoXO]*((?=xo)|(?=ox))[xoXO]*$', token.lower()):
        return 'xo'

    if re.search(r'(http|www)', token.lower()):
        return None

    token = mars2norm(token)
    if not token:
        return None

    if re.search(r'\d', token): # only contain digits
        return None
    if len(token) <= 2 or re.search(r'^(.)\1*$', token): 
        # only contain one character or repeat character
        return None
    if len(token) > 20: # and not enchant_dict.check(token.lower()):
        return None

    if token in stopset:
        return None
    
    token = stemmer.stemming(token.lower()).strip()
    if token in stopset:   # double check for unexpected text form, like 'marias'
        return None

    # if token.lower() in seed_words:
    #     return token.lower()

    # need to optimize
    if re.search(r'\d', token): # only contain digits
        return None
    if len(token) <= 2 or re.search(r'^(.)\1*$', token): 
        # only contain one character or repeat character
        return None
    if len(token) > 20: # and not enchant_dict.check(token.lower()):
        return None

    return token.lower()

############################################################
#   Helper
############################################################

def unescape(text):
    import re, htmlentitydefs
    def fixup(m):
        text = m.group(0)
        if text[:2] == "&#":
            # character reference
            try:
                if text[:3] == "&#x":
                    return unichr(int(text[3:-1], 16))
                else:
                    return unichr(int(text[2:-1]))
            except ValueError:
                pass
        else:
            # named entity
            try:
                text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
            except KeyError:
                pass
        return text # leave as is
    return re.sub("&#?\w+;", fixup, text)




# class Cleaner(object):

#     @staticmethod
#     def clean(content):
#         pass



