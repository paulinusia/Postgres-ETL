import psycopg2
import json
import re
import os
import pandas as pd
import numpy as np

con = psycopg2.connect(
    database='nlp_backup',
    password='password')
    
c = con.cursor()

def pull_and_convert_initial_comments():
    query = "SELECT * FROM initial_comment"
    c.execute(query)
    result = c.fetchall()
    col_names = []
    print('converting comments into dataframe...')
    for elt in c.description:
        col_names.append(elt[0])
    return pd.DataFrame(result, columns=col_names)
   
def pull_and_convert_replies():
    query = "SELECT * FROM replies"
    c.execute(query)
    result = c.fetchall()
    col_names = []
    print('converting replies into dataframe...')
    for elt in c.description:
        col_names.append(elt[0])
    return  pd.DataFrame(result, columns=col_names)

def write_out(filename, dataframe_column, df):
    with open(filename, 'a', encoding='utf8') as f:
                for content in df[dataframe_column].values:
                    f.write(str(content)+'\n')

#Retrieves cell locations for all strings/substrings
def find_df_value(dataframe, phrase):
    idx = dataframe.apply(lambda x: x.str.contains(phrase)).fillna(False)
    vals = idx[idx == True].stack().index.tolist()
    return vals

#finds ID in given column
def find_id(df, column, id):
    col = df[column]
    occr = []
    val = col[col == id].index
    for x in val:
        location = [x, column]
        occr.append(location)
    return occr

#returns one sorted dataframe
def sort_dataframes(comment, replies):
    df = pd.DataFrame(columns=['comment', 'reply'])
    for x in comment['comment_id']:
        print(x)
        '''
        occr = find_id(replies, 'parent_id', x)
        if
        print('occr 0...',occr[0][0])'''

        occr = find_df_value(comment, x)
        print(occr)

        #get row for reply 
        reply_comment = replies.iloc[occr[0][0]]
        reply_comment_val = reply_comment['comment']
        #get index value for row
        idx =  comment.loc[comment['comment_id'] == x].index[0]
        #get row
        row = comment.iloc[idx]
        #get comment
        initial_comment = row['comment']
        df = df.append({'comment': initial_comment, 'reply': reply_comment_val}, ignore_index=True)
        df = re_censor(df)
    print(df)
    return df


def re_censor(df):
    '''
    FIND BAD WORDS LOCATION WITH FIND DF VALUES

    TAKE LIST OF BAD WORD LOCATIONS, FIND THE ROW, AND DF['COMMENT]

    for x in bad_words:
        for (x in location_list)
            row = x[0]
            column = x[1]
            if df[row][column] contains x
            df.val.replace({x:'*censored*'}, regex=True)

    '''
    with open('./filter_lists/redacted.txt', buffering=1000) as f:
        for row in f:
            occr = find_df_value(df, row)
            for x in occr:
                if  row in df[occr[0]]['comment']:
                    df = df[occr[0]]['comment'].replace({x: '*censored*'}, regex=True)
                if row in df[occr[0]]['reply']:
                    df = df[occr[0]]['reply'].replace({x: '*censored*'}, regex=True)
    return df
    


#-----------------------------------------------------------------------------------------------------


print('pulling comments....')
comment = pull_and_convert_initial_comments()
print('pulling replies...')
replies = pull_and_convert_replies()



#WRITE DATAFRAMES OUT TO FILE BEFORE PROCEEDING WITH SEARCH FUNCTION
write_out('../output_files/comments_unsorted.csv', 'comment', comment)
write_out('../output_files/replies_unsorted.csv', 'comment', replies)


comment = comment.drop('utc', axis=1)
comment = comment.drop('score', axis=1)
replies = replies.drop('utc', axis=1)
replies = replies.drop('score', axis=1)

print(comment.head())
print(replies.head())
sorted_df = sort_dataframes(comment, replies)

sorted_df.to_csv('./output_files/master.csv', encoding='utf-8', index=False)

write_out('./output_files/comments.csv', 'comment', comment)
write_out('./output_files/replies.csv', 'comment', replies)



