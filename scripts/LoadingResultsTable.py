
# coding: utf-8

# In[2]:

import os
import pymysql
import csv


# In[3]:

#connect to "dig ocean" machine
conn = pymysql.connect(host='104.131.83.145', port=3306, user='priyav', passwd='XpriyavF403')
cur = conn.cursor()
sql = "show databases;"
cur.execute(sql)


# In[ ]:

#Read the finalResults_1973_163frusTelegrams.csv file. This file has mapping of 163 frus telegrams for the year 1973. 
#There are 1479222 records in finalResults_1973_163frusTelegrams.csv file. This will be loaded into "results" table of
#"etc" database.
with open('files/results/finalResults_1973_163frusTelegrams.csv','r') as f:
    lines=csv.reader(f.read().splitlines())
    for i,line in enumerate(lines):
        if i > 0:
            if i%1000==0:print "%d documents processed"%i
            cosine_index = i
            sdc_docid = int(line[2])
            cosine_sim_value = float(line[6])
            frus_docid = line[8]
            sql_frus = "select date from declassification_frus.docs where id = "'"%s"'""%frus_docid
            cur.execute(sql_frus)
            result = cur.fetchone()
            frus_date = result[0]
            sql_sdc = "select DATESQL,DOC_NBR from declassification.statedeptcables where DOCID=%d;"%sdc_docid
            cur.execute(sql_sdc)
            result = cur.fetchone()            
            sdc_date = result[0]
            sdc_doc_nbr = result[1]

            #insert into results table of etc database
            sql = "insert into etc.results (cosine_index,cosine_sim_value,frus_docid,sdc_docid,frus_date,sdc_date,sdc_doc_nbr) values (%d,%f,"'"%s"'",%d,"'"%s"'",%d,"'"%s"'")"%(i,cosine_sim_value,frus_docid,sdc_docid,frus_date,sdc_date,sdc_doc_nbr)
#            print sql
            cur.execute(sql)
            conn.commit()


