import pymysql
import database as frus 
import pandas as pd

def gen_frus_corpus(date_year,date_month):
	charset = 'utf8'
	host_name = '104.131.83.145'
	db = 'declassification_frus'
	password = 'XpriyavF403'
	user = 'priyav'
	
#Build sql 
	sql = "select id,title,subject,date,p_from,p_to,body from docs \
			where year(date) = %d and month(date) = %d \
			and id in \
			('frus1969-76v27d111', 'frus1969-76v37d45','frus1969-76ve03d104','frus1969-76ve06d139','frus1969-76ve03d105', \
			'frus1969-76v30d37','frus1969-76v27d274','frus1969-76v27d279','frus1969-76v30d38','frus1969-76ve14p1d176', \
			'frus1969-76ve08d192','frus1969-76v27d283','frus1969-76v27d284','frus1969-76ve03d174','frus1969-76ve12d121', \
			'frus1969-76v37d50');"%(int(date_year),int(date_month))


	#sql = sql + r' and lower(title) like "%telegram%";'		
	print sql
	DB = frus.DBCONNECT(host_name, db, user, password,charset)
	DB.conn.autocommit(1)
	DB.cursor.execute(sql)
	data = DB.cursor.fetchall()

	results = {'id':[],'title':[],'subject':[],'date':[],'p_from':[],'p_to':[],'body':[]}
	for i,row in enumerate(data):
		#print i
		id_ = str(row['id'])
		title = str(row['title'])
		subject = str(row['subject'])
		date = row['date']
		p_from = str(row['p_from'])
		p_to = str(row['p_to'])
		body = row['body'].encode("utf8")

		results['id'].append(id_)
		results['title'].append(title)
		results['subject'].append(subject)
		results['date'].append(date)
		results['p_from'].append(p_from)
		results['p_to'].append(p_to)
		results['body'].append(body)

	df_results = pd.DataFrame.from_dict(results,orient='columns') #create a pandas dataframe
	file_name = 'frus_telegrams_' + date_year + date_month +'.csv' 
	#df_results.to_csv(file_name)	
	#print df_results.columns.values
	return df_results

if __name__ == "__main__":
	date_year = '1973'
	date_month = '01'

	df = gen_frus_corpus(date_year,date_month)		
