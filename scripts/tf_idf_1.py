
# coding: utf-8

# In[2]:

import os
from math import log,pow,sqrt
import re
import pandas as pd
import numpy as np
from collections import defaultdict
import nltk
from nltk.stem.porter import *
import pymysql


# In[3]:

#This function takes "document body" as input and performs the following.
# 1. convert into lower case
# 2. remove "\\n" characters
# 3. remove stop words
# 4. perform word stemming. Currently trying porter stemmer.
# 5. lemmentation ??? -- Not explored yet
def data_reformat(cleanBody):
    reformated_data = cleanBody.lower()
    reformated_data = re.sub(r'\n','',reformated_data)
    reformated_data = re.sub(r'[^a-zA-Z0-9: ]','',reformated_data)    
    words_list = reformated_data.split()
    reformated_list = [w for w in words_list if not w in nltk.corpus.stopwords.words('english')]
    stem_results=[]
    stemmer = PorterStemmer()
    for word in reformated_list:
        stem_results.append(str(stemmer.stem(word)))
    reformated_document = ' '.join(stem_results)
    return reformated_document


# In[4]:

def data_reformat_noStemming(cleanBody):
    reformated_data = cleanBody.lower()
    reformated_data = re.sub(r'\n','',reformated_data)
    reformated_data = re.sub(r'[^a-zA-Z0-9: ]','',reformated_data)    
    words_list = reformated_data.split()
    reformated_list = [w for w in words_list if not w in nltk.corpus.stopwords.words('english')]
    reformated_document = ' '.join(reformated_list)
    return reformated_document


# In[5]:

#function to calculate the term frequency within a given document. 
#normalized_tf = (# of times of occurance of term in a given doc)/total # of words in the document
def calc_normalized_tf(term,document):
    words = document.lower().split()
    normalized_tf = words.count(term.lower())/float(len(words))
    return normalized_tf
#print calc_normalized_tf("",document1)
        


# In[6]:

#Function to calculate the inverse document frequency (idf). For example, idf for the term "game" is 
#idf = 1 + log(Total number of documents/Number of documents with the term "game" in it)
def calc_idf(term,documents):
    no_of_docs_with_term = 0
    for doc in documents:
        if term in doc.split():
            no_of_docs_with_term = no_of_docs_with_term + 1
    if no_of_docs_with_term > 0:
        return 1.0 + log(float(tot_no_of_documents)/no_of_docs_with_term)
    else:
        return 1.0
#print calc_idf("game",documents)


# In[7]:

#Build similarity matrix (tf*idf)
def build_sim_matrix(document_index):
    sim_table = np.zeros((tot_no_of_query_terms,tot_no_of_query_terms))
    for i in range(tot_no_of_query_terms):
        sim_table[i,0] = normalized_tf_doc0[i,0] * idf_table[i,0]
        sim_table[i,1] = tf_table[i,document_index] * idf_table[i,0]
#    print sim_table
    return sim_table


# In[8]:

#calculate cosine-similarity
def calc_cosine_similarity(arr):
    (m,n) = arr.shape #Notice that n is always 2 here, since we are calculating the similarity between two documents at any given point
    numerator_value = 0
    denominator_value = 0
    for i in range(m):
        numerator_value = numerator_value + (arr[i,0] * arr[i,1])
    temp1 = 0
    temp2 = 0
    for i in range(m):
        temp1 = temp1 + pow(arr[i,0],2)
        temp2 = temp2 + pow(arr[i,1],2)
    denominator_value = sqrt(temp1) * sqrt(temp2)
#if there are no matching terms between a document and a query, the tf values are all zero and hence the tf*idf 
#values will be zero. This implies the documents are complete dis-similar and hence the cosine similarity = -1
    if denominator_value == 0.0:
        return -1.0
    else:
        return float(numerator_value)/denominator_value


# In[9]:

#Build tf-table. This table contains the query term and the corresponding tf for each document. 
#size of this table = no.of.query terms x no.of.documents
def build_tf_table():
    tf_table = np.zeros((tot_no_of_query_terms,tot_no_of_documents))
    print tot_no_of_documents
    #print tot_no_of_query_terms
    for i in range(tot_no_of_query_terms):
        for j in range(tot_no_of_documents):

            tf_table[i,j] = calc_normalized_tf(terms[i],documents[j])
    return tf_table


# In[10]:

#Build idf_table. This table contains the idf value for each of the query term.
#size of this table = no.of.query terms x 1 column
def build_idf_table():
    idf_table = np.ones((tot_no_of_query_terms,1))
    for i in range(tot_no_of_query_terms):
        idf_table[i,0] = calc_idf(terms[i],documents)
    return idf_table


# In[11]:

#Build tf_idf_table. This table has tf*idf values for each of the query term and for each document.
#size of this table = no.of.query terms x no.of.documents 
def build_tf_idf_table():
    tf_idf_table = np.zeros((tot_no_of_query_terms,tot_no_of_documents))
    for i in range(tot_no_of_query_terms):
        for j in range(tot_no_of_documents):
            tf_idf_table[i,j] = tf_table[i,j] * idf_table[i,0]
    return tf_idf_table


# In[12]:

#To calculate the cosine similarity between query and document1, consider the query as a document0
# and calculate the consine similarity between document0 and document1

#To calculate the normalized_tf of document0
def build_normalized_tf_query():
    normalized_tf_doc0 = np.zeros((tot_no_of_query_terms,1))
    for i in range(tot_no_of_query_terms):
        normalized_tf_doc0[i,0] = calc_normalized_tf(terms[i],query)
    return normalized_tf_doc0


# In[13]:

def build_cosine_similarity_result():
    cosine_similarity_result = []
    for i in range(tot_no_of_documents):
        sim_table = build_sim_matrix(i)
        cosine_similarity_result.append(calc_cosine_similarity(sim_table))
#    print cosine_similarity_result 
    return cosine_similarity_result   


# In[16]:
curr_date = [197301]
for p in range(len(curr_date)):
    input_file_name = 'files/input/sdc/output_uniq_words_' + str(curr_date[p]) + '.csv'
    documents_with_unique_words = pd.read_csv(input_file_name)
    #documents_with_unique_words = pd.read_csv('sdc_files/output_uniq_words_197301.csv')

    input_file_name = 'files/input/sdc/output_' + str(curr_date[p]) + '.txt'
    print input_file_name
    documents_with_cleanBody = pd.read_table(input_file_name)

    #Extract cleanBody and append it with the documents_with_unique_words['cleanBody']
    body = documents_with_cleanBody[documents_with_cleanBody['DOCID'].isin(documents_with_unique_words['docid'])]['cleanBody']

    for i in range(len(documents_with_unique_words.index)):
        documents_with_unique_words.loc[i,'cleanBody'] = body.iloc[i]

    #create a set of reformated documents by removing STOP words. Also stemming is performed
    # These reformated documents will be used for further processing through tf-idf and hence to calcuate cosine similarity
    reformated_documents = []
    for i in range(len(documents_with_unique_words.index)):
        result = data_reformat(documents_with_unique_words.iloc[i]['cleanBody'])
        reformated_documents.append(result)

    for i in range(len(reformated_documents)):
        documents_with_unique_words.loc[i,'reformated_document'] = reformated_documents[i]

    documents = reformated_documents

    global tot_no_of_documents,tot_no_of_query_terms
    tot_no_of_documents = len(documents)

    #For each frus telegram issued in 197301, calcuate the cosine similarity with state dept cables issued in 197301
    frus_file_name = 'files/input/frus/frus_temp_' + str(curr_date[p]) + '.txt'
    frus_temp = pd.read_table(frus_file_name)
    #frus_temp = pd.read_table('frus_temp/frus_temp_197301.txt')
    frus_Vs_sdc_cosine = {'cosine_sim_value':[],'frus_docid':[],'frus_body':[],'sdc_docid':[],'sdc_cleanBody':[]}
    print "Total number of frus telegrams for year %d = %d"%(curr_date[p],len(frus_temp.index))
    for i in range(len(frus_temp.index)):
        print "frus document number = %d"%i
        frus_body = frus_temp.iloc[i]['body']
        frus_docid = frus_temp.iloc[i]['id']
        query = data_reformat(frus_body)
        terms = query.lower().split()
        tot_no_of_query_terms = len(terms)

        tf_table = build_tf_table() #build normalized tf-table of query with respect to document in statedeptcables
        idf_table = build_idf_table() #build idf-table
        tf_idf_table = build_tf_idf_table() #build tf*idf table
        normalized_tf_doc0 = build_normalized_tf_query() #build normalized tf of the query within itself
        cosine_similarity_result = build_cosine_similarity_result() #build the cosine_similarity_result
        
        for j in range(len(cosine_similarity_result)):
            documents_with_unique_words.loc[j,'cosine_similarity'] = cosine_similarity_result[j]
        final_result = pd.DataFrame.sort(documents_with_unique_words,columns='cosine_similarity',ascending=False)
        final_result.loc[:,'frus_body'] = frus_body
        final_result.loc[:,'frus_docid'] = frus_docid
        file_name = 'files/results/' + str(frus_docid) + '_'+ str(curr_date[p]) + '_with_stemming.csv'
        final_result.to_csv(file_name)

        for k in range(5):
            frus_Vs_sdc_cosine['cosine_sim_value'].append(final_result.iloc[k]['cosine_similarity'])
            frus_Vs_sdc_cosine['frus_docid'].append(final_result.iloc[k]['frus_docid'])
            frus_Vs_sdc_cosine['frus_body'].append(final_result.iloc[k]['frus_body'])
            frus_Vs_sdc_cosine['sdc_docid'].append(final_result.iloc[k]['docid'])
            frus_Vs_sdc_cosine['sdc_cleanBody'].append(final_result.iloc[k]['cleanBody'])
            #sql = "insert into results_temp (cosine_sim_value,frus_docid,frus_body,sdc_docid,sdc_cleanBody) values (%f,'"'%s'"','"'%s'"',%d,'"'%s'"')"%(final_result.iloc[k]['cosine_similarity'],final_result.iloc[k]['frus_docid'],str(final_result.iloc[k]['frus_body']),final_result.iloc[k]['docid'],str(final_result.iloc[k]['cleanBody']))
            #cur.execute(sql)
            #conn.commit()
    df_frus_Vs_sdc_cosine_sorted = pd.DataFrame.from_dict(frus_Vs_sdc_cosine,orient='columns')
    output_file_name = 'files/results/cosine_similarity_top5'+ '_'+ str(curr_date[p]) + '_with_stemming.csv'
    print output_file_name
    df_frus_Vs_sdc_cosine_sorted.to_csv(output_file_name)
    print "execution complete for %d"%curr_date[p]




    # In[233]:

    #Common words between the two most relevant documents
    #sdc_words_list = data_reformat(df_frus_Vs_sdc_cosine_sorted.iloc[0]['sdc_cleanBody']).lower().split()
    #terms = data_reformat(df_frus_Vs_sdc_cosine_sorted.iloc[0]['frus_body']).lower().split()
    #x = list(set(terms) & set(sdc_words_list))
    #print len(x)
    #print x

