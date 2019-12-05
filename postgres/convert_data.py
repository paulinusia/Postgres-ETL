import psycopg2
import json
import re
import os
import pandas as pd

con = psycopg2.connect(
    database='nlp_db',
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


def find_df_value(string):
    return


#returns one sorted dataframe
def sort_dataframes(comment, replies):
    '''
    PSEUDO CODE:
    for row in comment, get column 'comment_id'

         find_dataframe_value(comment_id) in column 'parent_id'
            get row(print row)

    new df:
    headers = comment, reply
    df. add comment [column ]
    write_out(df combined)
    '''


def read_csv(csv):
    return

def re_censor(bad_words_file, DF):
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

#-----------------------------------------------------------------------------------------------------


print('pulling comments....')
comment = pull_and_convert_initial_comments()
print('pulling replies...')
replies = pull_and_convert_replies()



#WRITE DATAFRAMES OUT TO FILE BEFORE PROCEEDING WITH SEARCH FUNCTION
write_out('./output_files/comments_unsorted.csv', 'comment', comment)
write_out('./output_files/replies_unsorted.csv', 'comment', replies)


print(comment.head())
print(replies.head())


