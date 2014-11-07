from BeautifulSoup import BeautifulSoup
from pandas import DataFrame
def separated_xml(infile):
    file = open(infile, "r")
    buffer = [file.readline()]
    for line in file:
        if line.startswith("<sasdoc "):
            yield "".join(buffer)
            buffer = []
        buffer.append(line)
    yield "".join(buffer)
    file.close()

names=['date','doc_nbr','subject','to','from','msgtext','type','film','capture_date','doc_source','doc_unique_id','legacy_key','locator','origclass','review_action','review_date','review_history','review_markings','tags','markings']
output_list = []

for xml_string in separated_xml("TEL.APR75.RPU.GP"):
    dict_temp = {}
    soup = BeautifulSoup(xml_string)
    for i in range(len(names)):
        for d in soup.findAll(names[i]):
            dict_temp[names[i]] = str(d.contents[0])
    output_list.append(dict_temp)    
        
print len(output_list)
df = DataFrame.from_dict(output_list,orient='columns')
print 'size of the final dataframe is '
print df.shape
#print df
output_file = open('TEL.APR75.RPU.GP.csv','w+')
#import os
#import stat
#os.chmod('test2.csv',stat_S_IXGRP)
#os.chmode('test2.csv',stat_S_IWOTH)
df.to_csv('TEL.APR75.RPU.GP.csv')
