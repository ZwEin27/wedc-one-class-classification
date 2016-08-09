# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-08-09 13:52:28
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-08-09 14:00:43

import os

from country import RES_COUNTRY
from nationality import RES_NATIONALITY
from male import RES_MALE
from female import RES_FEMALE


def get_stopword_set():
    stops = get_stopwords() + get_punctuations()
    names = get_person_names()
    country, country_abbr = get_country_names()
    numbers = get_numbers()
    nationality = get_nationality_names()
    stop_set = set(stops) | set(numbers) | set(names) | set(nationality) | set(country) | set(country_abbr)
    return stop_set

def get_numbers():
    numbers = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 
                'ten', 'eleven', 'twelve', 'thirteen', 'fourteen', 'fifteen', 'sixteen', 'seventeen', 'eighteen', 'nineteen',
                'twenty', 'thirty', 'forty', 'fifty', 'sixty', 'seventy', 'eighty', 'ninety',
                'hundred', 'thousand', 'million', 'billion']
    return numbers

# def get_stopword_from_file(file_path):
#     names_list = []
#     with open(file_path) as f:
#         lines = f.readlines()
#         for line in lines:
#             names_list.append(line.strip().lower())
#     return names_list

def get_male_names():
    # path = os.path.join(__res_dir__, 'stop_words', 'male.txt')
    # return get_stopword_from_file(path)
    return RES_MALE

def get_female_names():
    # path = os.path.join(__res_dir__, 'stop_words', 'female.txt')
    # return get_stopword_from_file(path)
    return RES_FEMALE

def get_person_names():
    # result = []
    # result.extend(get_male_names())
    # result.extend(get_female_names())
    return RES_MALE + RES_FEMALE

def get_country_names():
    # path = os.path.join(__res_dir__, 'stop_words', 'country.txt')
    # country_list = []
    # country_abbr_list = []
    # with open(path) as f:
    #     lines = f.readlines()
    #     for line in lines:
    #         abbr, country = line.strip().lower().split('|')
    #         country_list.append(country)
    #         country_abbr_list.append(abbr)
    country_list = []
    country_abbr_list = []
    for line in RES_COUNTRY:
        abbr, country = line.strip().lower().split('|')
        country_list.append(country)
        country_abbr_list.append(abbr)
    return country_list, country_abbr_list


def get_nationality_names():
    # path = os.path.join(__res_dir__, 'stop_words', 'nationality.txt')
    # return get_stopword_from_file(path)
    return RES_NATIONALITY

def get_stopwords(stops=None):
    custom_stops = [u'i', u'me', u'my', u'myself', u'we', u'our', u'ours', u'ourselves', u'you', u'your', u'yours', u'yourself', u'yourselves', u'he', u'him', u'his', u'himself', u'she', u'her', u'hers', u'herself', u'it', u'its', u'itself', u'they', u'them', u'their', u'theirs', u'themselves', u'what', u'which', u'who', u'whom', u'this', u'that', u'these', u'those', u'am', u'is', u'are', u'was', u'were', u'be', u'been', u'being', u'have', u'has', u'had', u'having', u'do', u'does', u'did', u'doing', u'a', u'an', u'the', u'and', u'but', u'if', u'or', u'because', u'as', u'until', u'while', u'of', u'at', u'by', u'for', u'with', u'about', u'against', u'between', u'into', u'through', u'during', u'before', u'after', u'above', u'below', u'to', u'from', u'up', u'down', u'in', u'out', u'on', u'off', u'over', u'under', u'again', u'further', u'then', u'once', u'here', u'there', u'when', u'where', u'why', u'how', u'all', u'any', u'both', u'each', u'few', u'more', u'most', u'other', u'some', u'such', u'no', u'nor', u'not', u'only', u'own', u'same', u'so', u'than', u'too', u'very', u's', u't', u'can', u'will', u'just', u'don', u'should', u'now', u'd', u'll', u'm', u'o', u're', u've', u'y', u'ain', u'aren', u'couldn', u'didn', u'doesn', u'hadn', u'hasn', u'haven', u'isn', u'ma', u'mightn', u'mustn', u'needn', u'shan', u'shouldn', u'wasn', u'weren', u'won', u'wouldn', u'us', u'im']
    if stops:
        custom_stops += stops
    return custom_stops

def get_punctuations():
    import string
    return list(string.punctuation)


if __name__ == '__main__':
    print get_stopword_set()
