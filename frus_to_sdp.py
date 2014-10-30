#python script to compare the frus telegrams between 1973 and 1977 to statedepartment cables
import lcs
import pandas as pd
import difflib as db

#Read the dataframe of statedepartment cables "TEL.APR75.RPU.GP"
sdc_tel_apr75 = pd.read_csv('TEL.APR75.RPU.GP.csv')

#Telegrams between 1973 and 1977 from frus database
frus = pd.read_csv('telegrams_frus_1973_1977.csv')

#fetch just the message of the first record for initial testing
frus_body = frus.iloc[1]['body']
sdc_msgtext = sdc_tel_apr75.iloc[1]['msgtext']

#Loop through the first frus telegram with sdc_tel_apr75.rpu to find out the document with the longest search match

#frus_no_of_rows = len(frus.index)
#sdc_no_of_rows = len(sdc_tel_apr75.index)

#fuction to cleanup date. replace \n \s with ''
#replace \'s with 's

def data_cleanup(message):
    to_replace = ["\n","\s","\r","\t"," ","\x93","\x94"]
    for i in range(len(to_replace)):
        message = message.replace(to_replace[i],"")
    message = message.lower()    
    message = message.replace("\'s","'s")
    return message

#frus_body = data_cleanup(frus_body)
#sdc_msgtext = data_cleanup(sdc_msgtext)
import re

def extract_alphanum(message):
    message  = re.sub(r'\W+','',frus_body)
    message  = frus_body.lower()
    message  = frus_body.replace(" ",'')
    return message

frus_body = extract_alphanum(frus_body)

#print "no of rows in sdc %d" %sdc_no_of_rows
matching_strings = [{}]

import difflib as db
def longest_match(word1,word2):
    s = db.SequenceMatcher(None,word1,word2)
    result = s.find_longest_match(0,len(word1)-1,0,len(word2)-1)
    start = result[0]
    end = result[2] + result[0]
    return word1[start:end]

def fetch_longest_matching_strings(frus_body):
    sdc = sdc_tel_apr75.iloc[1:len(sdc_tel_apr75.index)]
    sdc_no_of_rows = len(sdc)
    for i in range(sdc_no_of_rows):
       # print i
        if i%200==0:print "%d docs processed" %i
        dict_temp = {}
        sdc_msgtext = sdc.iloc[i]['msgtext']
        sdc_msgtext = extract_alphanum(sdc_msgtext)
        sdc_doc_nbr = sdc.iloc[i]['doc_nbr']
        result = longest_match(frus_body,sdc_msgtext)

      #  result = lcs.lcs(str(frus_body),str(sdc_msgtext))

        dict_temp['sdc_msgtext'] = sdc_msgtext
        dict_temp['sdc_doc_nbr'] = sdc_doc_nbr
        dict_temp['match_string'] = result
        matching_strings.append(dict_temp)
    
    best_longest_match_len = 0
    best_match_doc_id = ''
    best_match_msgtext = ''
    best_match = {}
    
    for j in range(len(matching_strings)):
        if len(matching_strings[j]['match_string']) > best_longest_match_len:
            best_longest_match_len = len(matching_strings[j]['match_string'])
            best_match_doc_id = matching_strings[j]['sdc_doc_nbr']
            best_match_sdc_msgtext = matching_strings[j]['sdc_msgtext']
    
    best_match['match_string'] = best_longest_match_len
    best_match['sdc_msgtext'] = best_match_sdc_msgtext
    best_match['doc_id'] = best_match_doc_id

    return best_match
            

fetch_longest_matching_strings(frus_body)

