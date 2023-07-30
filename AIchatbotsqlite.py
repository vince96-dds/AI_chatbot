import sqlite3
import json
from datetime import datetime

#format of files

timeframe='2023-4' #format of file time fram


sql_transaction=[]
connection=sqlite3.connect("something.db".format(timeframe)) # must create database

cursor=connection.cursor()

def format_data(data):
    data=data.replace("\n","newlinechar").replace("\r","newlinechar").replace('"',"'")
    return data
def find_parent(pid):
    try:
        sql= "SELECT comment FROM parent_reply WHERE comment_id='{}' LIMIT 1".format(pid)
        cursor.execute(sql)
    
        res=cursor.fetchone()
        if res!= None:
            return res[0]
        
        else:
            return False
    except Exception as e:
        print("find parent",e)
        return False
    
    
#for the reddit data######

def find_existing_score():
    try:
        sql= "SELECT score FROM parent_reply WHERE comment_id='{}' LIMIT 1".format(pid)
        cursor.execute(sql)
    
        res=cursor.fetchone()
        if res!= None:
            return res[0]
        
        else:
            return False
    except Exception as e:
        print("find score",e)
        return False
    
def acceptable(data):
    if len(data.split(''))>50 or len(data)<1:
        return False
    elif len(data)>1000:
        return False
    elif data == '[deleted]' or data== '[removed]':
        return False
    else:
        return True
    
def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction)>1000:
        cursor.execute('BEGIIN TRANSACTION')
        for s in sql_transaction:
            try:
                cursor.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction=[]

#create table#####     
def create_table():
    cursor.execute("""CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY,
                   comment_id TEXT UNIQUE,
                   parent TEXT,
                   comment TEXT,
                   subreddit TEXT,
                   unix INT,
                   score INT)""")# format of data

if __name__=="__main__":
    create_table()
    
    row_counter=0
    paired_rows=0
    
    with open("pathfile".format.split('-'),buffer=1000) as f: #put the path of the file
        for row in f:
            #depends on format of file
            row_counter+=1
            row=json.loads(row)
            parent_id=row['parent_id']
            body=format_data(row['body'])
            created_utc=row['created_utc']
            score=row['score']
            comment_id=row["name"]
            subreddit=row['subreddit']
            
            
            parent_data=find_parent(parent_id)
         
        #based on a reddit data. reddit from the site 
        # https://www.reddit.com/r/datasets/comments/3bxlg7/i_have_every_publicly_available_reddit_comment/?st=j9udbxta&sh=69e4fee7
        # was made as the dataset
        # we might not need this 
        # It checks if the comment score is high so that will be displayed and replace the comment
        # that has a lower score
        
            if score >=2:
                if acceptable(body):
                    existing_comment_score=find_existing_score(parent_id)
                    if existing_comment_score:
                        if score>existing_comment_score:
                            ### not needed 
                            sql_insert_replace_comment()#includes transaction_bldr
                    else:
                        if parent_data:
                            ### not needed
                            sql_insert_has_parent()#includes transaction_bldr
                            paired_rows+=1
                        else:
                            ### not needed
                            sql_insert_no_parent()#includes transaction_bldr        
                            
            if row_counter%1000==0:
                print("Total rows read: {}, Paired rows: {}, Time: {}".format(row_counter,paired_rows,str(datetime.now())))                