import pymysql
import re
import generateFrus as genFrus
import generateCorpus as genCorpus
import generateCosineSimilarity as genCosineSim
import nltk
from nltk.stem.porter import *
import database as results_1974


#This function takes "document body" as input and performs the following.
# 1. convert into lower case
# 2. remove "\\n" characters
# 3. remove stop words
# 4. perform word stemming. Currently trying porter stemmer.
# 5. lemmentation ??? -- Not explored yet
def reformat_msg(body):
	reformated_data = body.lower()
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

def main(date_year,date_month):
	#Generate the frus telegrams for the given year and month combination
	frus_df = genFrus.gen_frus_corpus(date_year,date_month)
	#Generate the statedept cable corpus for the given year and month combination
	sdc_df = genCorpus.gen_corpus(date_year,date_month)

	#Generate the cosine similarity value for each of the frus document with respect to every document in the corpus.
	total_no_of_frus_documents = len(frus_df.index) 
	total_no_of_sdc_documents = len(sdc_df.index)
	print total_no_of_sdc_documents

	#reformat the sdc corpus by removing stop words etc
	reformated_documents = []
	for i in range(total_no_of_sdc_documents):
		reformated_msg = reformat_msg(sdc_df.iloc[i]['body'])
		reformated_documents.append(reformated_msg)
	#Add a column to sdc_df with "reformated message"	
	for i in range(len(reformated_documents)):
	    sdc_df.loc[i,'reformated_body'] = reformated_documents[i]

	documents = reformated_documents 

	#Set mysql parameters
	charset = 'utf8' 
	host_name = '104.131.83.145'
	db = 'etc'
	password = 'XpriyavF403'
	user = 'priyav'

	print "total_no_of_frus_documents = %d"%total_no_of_frus_documents
	for i in range(total_no_of_frus_documents):
		print "current frus document number = %d"%i
		#reformat the frus message 
		frus_body = frus_df.iloc[i]['body']
		frus_docid = frus_df.iloc[i]['id']
		frus_date = frus_df.iloc[i]['date']

		#calcuate the cosine similarity value 
		query = reformat_msg(frus_body)
		cosine_sim_result = genCosineSim.gen_cosine_similarity_result(query,documents,date_year,date_month)
		#print cosine_sim_result

		DB = results_1974.DBCONNECT(host_name, db, user, password,charset)
		DB.conn.autocommit(1)


		#Add a column to sdc_df with "reformated message"	
		for i in range(len(cosine_sim_result)):
			sdc_df.loc[i,'cosine_sim_value'] = cosine_sim_result[i]
			cosine_sim_value = cosine_sim_result[i]
			sdc_docid = sdc_df.iloc[i]['doc_id']
			sdc_doc_nbr = sdc_df.iloc[i]['doc_nbr']
			sdc_date = sdc_df.iloc[i]['datesql']

	    #update the "results_1974 table with the "cosine_sim_value" between frus and sdc corpus
	    #Build sql
			sql = 'insert ignore into results_1974 (cosine_index,cosine_sim_value,frus_docid,frus_date,sdc_docid,sdc_doc_nbr,sdc_date)\
					values(%d,%f,"'"%s"'","'"%s"'",%d,"'"%s"'",%d);'%(i,cosine_sim_value,frus_docid,frus_date,int(sdc_docid),sdc_doc_nbr,sdc_date)

			#print sql
			DB.cursor.execute(sql)



if __name__ == "__main__":
	date_year = '1976'
	date_month = ['12']
	print "year = %s"%date_year
	for i in range(len(date_month)):
		print "month = %s" %date_month[i]
		main(date_year,date_month[i])

