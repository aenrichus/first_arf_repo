# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from selenium import webdriver
import psycopg2
import time 
import glob
import pandas as pd
driver=webdriver.Chrome(r'/usr/local/bin/chromedriver')

## Twitter log in info and download the csv file
def fillform(driver):
    username = 'chrisliuxiqiao4'
    password = '19941113liulu'
    driver.find_element_by_xpath('//*[@id="page-container"]/div/div[1]/form/fieldset/div[1]/input').send_keys(username)
    driver.find_element_by_xpath('//*[@id="page-container"]/div/div[1]/form/fieldset/div[2]/input').send_keys(password)
    driver.find_element_by_xpath('//*[@id="page-container"]/div/div[1]/form/div[2]/button').click()
    driver.find_element_by_xpath('//*[@id="account-selector-form"]/ul/li[1]/div').click()
    driver.find_element_by_xpath('//*[@id="SharedNavBarContainer"]/div/div/ul[1]/li/a').click()
    driver.find_element_by_xpath('//*[@id="SharedNavBarContainer"]/div/div/ul[1]/li/ul/li[2]/a').click()
    driver.find_element_by_xpath('//*[@id="export"]/button/span[2]').click()
    time.sleep(30)
    driver.close()
    

## Locate the latest csv file downloaded
def locate_csv():
    ## the folder that the csv file downloaded csv to
    files = glob.glob('/Users/xiqiaoliu/Downloads/tweet*.csv')
    newest = files[0]
    return newest

# Restructure the dataset according to the table 
def trim_dataset():
    df = pd.read_csv(locate_csv())
    new_df = df.drop(df.iloc[:,14:20],axis=1)
    new_df.to_csv(locate_csv(),index=False)
    return locate_csv()
    
## Connect to Postgres 
def import_postgres():
    # import dataset into a temporary table first 
#    try:
    conn = psycopg2.connect(user="postgres",host="127.0.0.1",port="5432",database="ARF_DB",password="1234.")
    cur = conn.cursor()
    f = open(locate_csv())
    next(f)
    cur.copy_expert("copy temp from STDIN CSV",f) ## I cannot use cursor.copy_from() here; if the first row is header, use "STDIN CSV HEADER"
            
        
        # delete rows in "twitter_data" which have same 'twitter id'
    query_1 = 'delete from twitter_data where "Twitter id" in (select "Twitter id" from temp)'
    query_2 = 'insert into twitter_data select * from temp'
    query_3 = 'delete from temp'
    cur.execute(query_1)
    cur.execute(query_2)
    cur.execute(query_3)
    conn.commit()
    f.close()
#    except:
#        print('Failed to import dataset to temporary table')
        
  
    
    # disconnect with database
    cur.close()
    conn.close()
    
        
    

def main():
    driver.get('https://twitter.com/login?redirect_after_login=https%3A%2F%2Fads.twitter.com%2Flogin%3Fredirect_after_login%3Dhttps%253A%252F%252Fads.twitter.com%252Fuser%252Fthe_ARF%252Ftweets&hide_message=1')
    fillform(driver)
    trim_dataset()
    locate_csv()
    import_postgres()
 

main()