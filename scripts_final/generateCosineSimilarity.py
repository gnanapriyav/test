import pymysql
import re
import pandas as pd
import generateFrus as genFrus #del later
import generateCorpus as genCorpus #del later
import nltk 
from nltk.stem.porter import *
import numpy as np
from math import log,pow,sqrt

#function to calculate the term frequency within a given document. 
#normalized_tf = (# of times of occurance of term in a given doc)/total # of words in the document
def calc_normalized_tf(term,document):
	words = document.lower().split()
	normalized_tf = words.count(term.lower())/float(len(words))
	return normalized_tf
#print calc_normalized_tf("",document1)
        

#Function to calculate the inverse document frequency (idf). For example, idf for the term "game" is 
#idf = 1 + log(Total number of documents/Number of documents with the term "game" in it)
def calc_idf(term,documents):
	no_of_docs_with_term = 0
	tot_no_of_documents = len(documents)
	for doc in documents:
	    if term in doc.split():
	        no_of_docs_with_term = no_of_docs_with_term + 1
	if no_of_docs_with_term > 0:
	    return 1.0 + log(float(tot_no_of_documents)/no_of_docs_with_term)
	else:
	    return 1.0
#print calc_idf("game",documents)


#Build similarity matrix (tf*idf)
def build_sim_matrix(document_index,tf_table,idf_table,normalized_tf_doc0,tot_no_of_query_terms):
	sim_table = np.zeros((tot_no_of_query_terms,tot_no_of_query_terms))
	for i in range(tot_no_of_query_terms):
	    sim_table[i,0] = normalized_tf_doc0[i,0] * idf_table[i,0]
	    sim_table[i,1] = tf_table[i,document_index] * idf_table[i,0]
	#    print sim_table
	return sim_table


#calculate cosine-similarity
def calc_cosine_similarity(arr):
	(m,n) = arr.shape #Notice that n is always 2 here, since we are calculating the similarity between two documents at any given point
	numerator_value = 0
	denominator_value = 0
	for i in range(m):
	    numerator_value = numerator_value + (arr[i,0] * arr[i,1]) #dot product of vectors.
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


#Build tf-table. This table contains the query term and the corresponding tf for each document. 
#size of this table = no.of.query terms x no.of.documents
def build_tf_table(terms,tot_no_of_query_terms,tot_no_of_documents,documents):
	tf_table = np.zeros((tot_no_of_query_terms,tot_no_of_documents))
	for i in range(tot_no_of_query_terms):
	    for j in range(tot_no_of_documents):
	        tf_table[i,j] = calc_normalized_tf(terms[i],documents[j])
	return tf_table


#Build idf_table. This table contains the idf value for each of the query term.
#size of this table = no.of.query terms x 1 column
def build_idf_table(terms,tot_no_of_query_terms,documents):
	idf_table = np.ones((tot_no_of_query_terms,1))
	for i in range(tot_no_of_query_terms):
	    idf_table[i,0] = calc_idf(terms[i],documents)
	return idf_table


#Build tf_idf_table. This table has tf*idf values for each of the query term and for each document.
#size of this table = no.of.query terms x no.of.documents 
def build_tf_idf_table(tot_no_of_query_terms,tot_no_of_documents,tf_table,idf_table):
	tf_idf_table = np.zeros((tot_no_of_query_terms,tot_no_of_documents))
	for i in range(tot_no_of_query_terms):
	    for j in range(tot_no_of_documents):
	        tf_idf_table[i,j] = tf_table[i,j] * idf_table[i,0]
	return tf_idf_table


#To calculate the cosine similarity between query and document1, consider the query as a document0
# and calculate the consine similarity between document0 and document1

#To calculate the normalized_tf of document0
def build_normalized_tf_query(query,terms,tot_no_of_query_terms):
	normalized_tf_doc0 = np.zeros((tot_no_of_query_terms,1))
	for i in range(tot_no_of_query_terms):
	    normalized_tf_doc0[i,0] = calc_normalized_tf(terms[i],query)
	return normalized_tf_doc0


def build_cosine_similarity_result(tot_no_of_documents,tf_table,idf_table,normalized_tf_doc0,tot_no_of_query_terms):
	cosine_similarity_result = []
	for i in range(tot_no_of_documents):
	    sim_table = build_sim_matrix(i,tf_table,idf_table,normalized_tf_doc0,tot_no_of_query_terms)
	    cosine_similarity_result.append(calc_cosine_similarity(sim_table))
	#    print cosine_similarity_result 
	return cosine_similarity_result   

#This function will take a frus document and sdc_corpus as input and calcuates the cosine similarity between the 
#given frus doc and each of the sdc document in the corpus
def gen_cosine_similarity_result(query,documents,date_year,date_month):
	terms = query.lower().split()
	tot_no_of_query_terms = len(terms)
	tot_no_of_documents = len(documents)

	tf_table = build_tf_table(terms,tot_no_of_query_terms,tot_no_of_documents,documents) #build normalized tf-table of query with respect to document in statedeptcables
	idf_table = build_idf_table(terms,tot_no_of_query_terms,documents) #build idf-table
	#	tf_idf_table = build_tf_idf_table(tot_no_of_query_terms,tot_no_of_documents,tf_table,idf_table) #build tf*idf table
	normalized_tf_doc0 = build_normalized_tf_query(query,terms,tot_no_of_query_terms) #build normalized tf of the query within itself

	cosine_similarity_result = build_cosine_similarity_result(tot_no_of_documents,tf_table,idf_table,normalized_tf_doc0,tot_no_of_query_terms) #build the cosine_similarity_result
	return cosine_similarity_result

#if __name__ == '__main__':
	

	
