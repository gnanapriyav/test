import pymysql
import csv
import database as sdc #sdc for statedeptcables
import pandas as pd

def gen_corpus(date_year,date_month):
	#set the mysql login parameters	-- testing
	charset = 'utf8'
	host_name = '104.131.83.145'
	db = 'declassification'
	password = 'XpriyavF403'
	user = 'priyav'
#Build sql
	sql = "select DOCID,DOC_NBR,DATESQL,cleanBody from statedeptcables where substring(DATESQL,1,6) = %d and cleanBody IS NOT NULL \
			and length(cleanBody) > 0;" %(int(date_year + date_month))
	#print sql

	DB = sdc.DBCONNECT(host_name, db, user, password,charset)
	DB.conn.autocommit(1)
	DB.cursor.execute(sql)
	data = DB.cursor.fetchall()
	
	results = {'doc_id':[],'body':[],'doc_nbr':[],'datesql':[]}
	for counter, row in enumerate(data):
	    #print counter
	    #if counter%500==0: print "done with %s rows"%counter 
	    doc_id = str(row['DOCID'])
	    body = row['cleanBody']
	    doc_nbr = row['DOC_NBR']
	    datesql = row['DATESQL']

	    #append the results to the dictionary "results". This dictionary will later be converted into pandas data and stored as csv
	    results['doc_id'].append(doc_id)
	    results['body'].append(body)
	    results['doc_nbr'].append(doc_nbr)
	    results['datesql'].append(datesql)

	    #fetch the next row from the mysql database
	    data = DB.cursor.fetchone()

	#write the results to a csv file.
	file_name = 'sdc_' + date_year + date_month + '.csv'
	df_results = pd.DataFrame.from_dict(results,orient='columns')
	#df_results.to_csv(file_name) 
	return df_results


if __name__ == "__main__":

	date_year = '1973'
	date_month = '01'

	df = gen_corpus(date_year,date_month)		



