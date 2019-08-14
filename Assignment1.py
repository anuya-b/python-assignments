#!/usr/bin/env python
# coding: utf-8

# In[1]:


#required pakages
import pandas as pd
import glob
import csv
from tabulate import tabulate
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


# #### 1
# #### Write class which has at two methods:

# In[2]:


#### class to get the input from user and display the string
class Test: 
    def get_input(self): # to get a string from console input
        self.name=input('Please enter any string:')

    def print_output(self): #to print the string in lower case.
        lowName=self.name.lower()
        print('Entered string: ',self.name)
        print('Entered string in lower case: ',lowName)


# In[3]:


t1=Test()
t1.get_input()
t1.print_output()


# #### 2
# #### Write code to merge data from 2 CSV files in single file

# In[4]:


data_1=pd.read_csv(r'Data/small-01.csv')
display(data_1.head())
data_2=pd.read_csv(r'Data/small-02.csv')
display(data_2.head())
result_df=pd.concat([data_1,data_2],ignore_index=True)
display(result_df)


# In[5]:


## in general
directory=r'Data/'
filenames=[filename for filename in glob.glob(directory+'*.csv')]
print(filenames)
new_df=pd.concat([pd.read_csv(file) for file in filenames],ignore_index=True,sort=False)
display(new_df)


# #### 3
# #### Write code to load data from CSV file into DB

# In[6]:


import pymysql


# In[7]:


host='localhost'
user='ragashri'
password=''
databaseName='dB1'

conn=pymysql.connect(host,user,password,local_infile=True)

conn_cursor=conn.cursor()

sqlStatement='CREATE DATABASE IF NOT EXISTS '+databaseName

conn_cursor.execute(sqlStatement)

sqlQuery='SHOW DATABASES'

conn_cursor.execute(sqlQuery)

dbNames=conn_cursor.fetchall()

for name in dbNames:
    print(name)


# In[8]:


conn_cursor.execute('USE '+databaseName)


# In[ ]:


# CREATE TABLE IF NOT EXISTS titanicData(
#  PassengerId INT NOT NULL,
#  Survived INT ,
#  Pclass INT,
#  Name VARCHAR(255),
#  Sex VARCHAR(10),
#  Age INT
# );


# In[9]:


conn_cursor.execute('SET GLOBAL local_infile = 1;')


# In[10]:


conn_cursor.execute("SHOW VARIABLES LIKE 'local_infile';")


# In[11]:


conn_cursor.execute(
    """LOAD DATA LOCAL INFILE 'D:/DS INTERNAL/TINUITI/Python/Assignments-Pravin/Data/train_titanic.csv' 
    INTO TABLE titanicData 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS ;""")


# In[12]:


sqlQuery='SHOW TABLES'

conn_cursor.execute(sqlQuery)

dbNames=conn_cursor.fetchall()

for name in dbNames:
    print(name)


# In[13]:


conn_cursor.execute('SELECT * FROM titanicdata')
results=conn_cursor.fetchall()
# for result in results:
#     display(result)
print(tabulate(results,headers=['PassengerId','Survived','Pclass','Name','Sex','Age'],tablefmt='grid'))


# In[14]:


conn.commit()


# #### 4

# #### Suppose CSV file provides campaign_id, campaign_name, impressions, clicks, date etc. Then write code to -
# #####                 - Read data from CSV files
# #####                 - Store in DB table as raw data
# #####                 - Apply transformation and store into new DB table
# #####                                 - Add "Campaign:" as prefix to campaign_name
# #####                                 - Calculate CTR and store into new field
# 

# In[15]:


campaignData=pd.read_csv(r'Data/campaignData.csv')
display(campaignData)


# In[16]:


campaignData['date']=pd.to_datetime(campaignData['date'],format='%d/%m/%Y')


# In[17]:


#relevant import:
from sqlalchemy import create_engine

#accessing database:
engine = create_engine("mysql+pymysql://root:root@localhost/dB1")
con = engine.connect()


# In[18]:


#uploading dataframe to database:
campaignData.to_sql(con=con, name='campaigntable',if_exists='replace') #no need to create table


# In[19]:


conn_cursor.execute('SELECT * FROM campaigntable')
results=conn_cursor.fetchall()
print(tabulate(results,headers=['campaign_id','campaign_name','impressions','clicks','date'],tablefmt='grid'))


# In[20]:


campaign_new=campaignData.copy()
display(campaign_new)


# In[21]:


campaign_new['campaign_name']='Campaign:'+campaign_new['campaign_name']
display(campaign_new)


# In[22]:


campaign_new['CTR']=round(campaign_new['clicks']/campaign_new['impressions'],3)
display(campaign_new)


# In[23]:


#uploading dataframe to database:
campaign_new.to_sql(con=con, name='campaignnew',if_exists='replace') #no need to create table


# In[27]:


conn_cursor.execute('SHOW TABLES')
dbTables=conn_cursor.fetchall()
print(dbTables)


# In[30]:


# conn_cursor.execute('SELECT * FROM campaignnew')
# results=conn_cursor.fetchall()
# print(tabulate(results,headers=['campaign_id','campaign_name','impressions','clicks','date','CTR'],tablefmt='grid'))


# #### 5

# In[31]:


data=pd.read_csv(r'Data/dummy.csv')
display(data)


# In[ ]:


# data.loc[:,data.columns[1:]]


# In[ ]:


# testRow=data['keys'][0]
# print(testRow)
# print(type(testRow))
# test=testRow.strip('][').split(',') 
# print(test,type(test))
# # test1=test.strip('\'')
# # print(test1,type(test1))



# In[ ]:


# testRow[0:-1].split(',')


# In[ ]:


# newTest=[word.strip('\'').split(',') for word in test]
# print(newTest)


# In[ ]:


# for i in range(len(newTest)):
#     newTest[i]


# In[32]:


df=data.copy()
df.drop(columns='keys',inplace=True)
for i in range(len(data['keys'][0].split(','))):
    df[i]=data['keys'].apply(lambda row: row[1:-1].split(',')[i])
display(df)
print(df.columns)
df=df.reindex(columns=[0,1,2,3,'clicks','impressions','ctr','position'])
display(df)
desired_col=['keys','website','device','country','clicks','impressions','ctr','position']
df.columns=desired_col
display(df)              


# #### 6

# #### Create 2 DB tables 
# ####                - tbl_clicks - campaign_id, clicks, date
# ####                - tbl_impressions - campaign_id, impressions, date
# ####              - Add some dummy data
# ####               - Write code to -
# ####                                - Show Impressions, clicks, CTR against each campaign per date for last 5 days
# ####                                - Show Impressions, clicks, CTR against each campaign
# ####                                - Show Total Impressions, clicks, CTR
# 

# ##### Method I: using SQL 

# In[195]:


# Show Impressions, clicks, CTR against each campaign per date for last 5 days

query1= '''SELECT * FROM (SELECT c.campaign_date 'CAMPAIGN_DATE',c.campaign_id 'CID',c.clicks 'Clicks',i.impressions 'Impressions',dense_rank() OVER (PARTITION BY c.campaign_id ORDER BY c.campaign_date desc) "RANKKK"
FROM tbl_clicks c NATURAL JOIN tbl_impressions i
)AS A
WHERE RANKKK<6;'''

conn_cursor.execute(query1)
results_1=conn_cursor.fetchall()
# for result in results:
#     display(result)
print(tabulate(results_1,headers=['CAMPAIGN DATE','CAMPAIGN ID','CLICKS','IMPRESSIONS','CTR'],tablefmt='grid'))


# ##### Method II using Python pandas groupby 

# In[38]:


# Show Impressions, clicks, CTR against each campaign per date for last 5 days
# pd.read_sql_table('tbl_clicks',conn,'db1')
clicksData=pd.read_sql('SELECT * FROM tbl_clicks',conn)
impressionData=pd.read_sql('SELECT * FROM tbl_impressions',conn)
display(clicksData)
display(impressionData)


# In[140]:


result_data=pd.merge(clicksData,impressionData,on=['temp_no','campaign_id','campaign_date'],how='inner').sort_values(by='campaign_id')
display(result_data)


# In[184]:


result_data['campaign_date']=pd.to_datetime(result_data['campaign_date'])


# In[141]:


result_data['CTR']=round(result_data['clicks']/result_data['impressions'],3)
display(result_data)


# In[142]:


res=result_data.groupby('campaign_id').apply(lambda x:x['campaign_date'].sort_values(ascending=False)).to_frame()
display(res.groupby('campaign_id').head(5))


# In[143]:


temp_res=res.groupby('campaign_id').head(5).reset_index()


# In[144]:


print(list(temp_res['level_1']))
list(result_data['temp_no'])


# In[145]:


temp_res['level_1']=temp_res['level_1']+1


# In[187]:


result_data.drop('temp_no',axis=1,inplace=True)


# In[165]:


result_data.loc[result_data['temp_no'].isin(temp_res['level_1']),['campaign_id','campaign_date','clicks','impressions','CTR']]


# ##### Method -III using Python pandas df (Direct sorting)

# In[192]:


result_data.sort_values(['campaign_id','campaign_date'],ascending=[True,False])


# In[131]:


# pd.merge(temp_res,result_data,on=['campaign_id','level_1','campaign_date'])
# temp_res.merge(result_data.rename(columns={'temp_no':'level_1'}),on=['campaign_id','level_1'])
# ids=res.groupby('campaign_id').head(5)
# result_data.loc[pd.Index(ids)]


# In[151]:


# result_data.sort_values(by='campaign_date',ascending=False).groupby(['campaign_id']).head(5)


# ##### Method I: Using SQL

# In[196]:


# Show Impressions, clicks, CTR against each campaign

query2= """SELECT newtable.CID 'CAMPAIGN ID',SUM(newtable.Impressions) 'Total Impressions',SUM(newtable.Clicks) 'Total Clicks', sum(newtable.Clicks/newtable.Impressions) 'TOTAL CTR'
FROM
(
SELECT c.campaign_id 'CID',c.clicks 'Clicks',i.impressions 'Impressions'
FROM tbl_clicks c NATURAL JOIN tbl_impressions i
) as newtable
GROUP BY newtable.CID;"""

conn_cursor.execute(query2)
results_2=conn_cursor.fetchall()
# for result in results:
#     display(result)
print(tabulate(results_2,headers=['CAMPAIGN ID','TOTAL IMPRESSIONS','TOTAL CLICKS','TOTAL CTR'],tablefmt='grid'))


# ##### Method II : Using python pandas

# In[168]:


# Show Impressions, clicks, CTR against each campaign

res=result_data.groupby('campaign_id',as_index=False).agg({'clicks':sum,'impressions':sum,'CTR':sum})
display(res)


# In[197]:


# Show Total Impressions, clicks, CTR

query3= """SELECT SUM(newtable.Impressions) 'Total Impressions',SUM(newtable.Clicks) 'Total Clicks', sum(newtable.Clicks/newtable.Impressions) 'TOTAL CTR'
FROM
(
SELECT c.campaign_id 'CID',c.clicks 'Clicks',i.impressions 'Impressions'
FROM tbl_clicks c NATURAL JOIN tbl_impressions i
) as newtable;"""

conn_cursor.execute(query3)
results_3=conn_cursor.fetchall()
# for result in results:
#     display(result)
print(tabulate(results_3,headers=['TOTAL IMPRESSIONS','TOTAL CLICKS','TOTAL CTR'],tablefmt='grid'))


# In[198]:


# Show Total Impressions, clicks, CTR
result_data.loc[:,['clicks','impressions','CTR']].agg({'clicks':sum,'impressions':sum,'CTR':sum}).to_frame().T


# #### 7

# #### IF you have AWS test account write code to -
# ####                - Download raw file from S3 
# ####               - Store raw file on local system
# ####                - Load local file into redshift
# ####                - Then delete local file from system.
# 

# In[ ]:


import boto3


# In[ ]:


s3_object=boto3.client('s3',region_name='us-east-1',aws_access_key_id=AWS_ACCESS_KEY,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


# In[ ]:


s3_object.download_file('demostoragebucket','campaignData.csv',r'D:\DS INTERNAL\TINUITI\Python\Assignments-Pravin\AWS\Data\campaignData_aws.csv')


# In[ ]:





# In[ ]:





# In[ ]:




