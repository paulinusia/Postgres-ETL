import psycopg2
import json
import re
import os
import pandas as pd

con = psycopg2.connect(
    database='nlp_db',
    password='password')

c = con.cursor()


def remove_childless_parents():
    
    query = "SELECT * FROM initial_comment"
    c.execute(query)
    result = c.fetchall()
    col_names = []
    for elt in c.description:
        col_names.append(elt[0])
    df = pd.DataFrame(result, columns=col_names)
    df = df.iloc[:, 0].tolist()

    for comment_id in df:
        query = "SELECT parent_id from replies where parent_id = '{}' LIMIT 1".format(comment_id)
        c.execute(query)
        result = c.fetchone()
        #print(result)
        if result == None:
            query = "DELETE from initial_comment WHERE comment_id = '{}'".format(comment_id)
            c.execute(query)
            con.commit()





def remove_possible_duplicates():
    query = "SELECT * FROM replies"
    c.execute(query)
    result = c.fetchall()
    col_names = []

    for elt in c.description:
        col_names.append(elt[0])
    df = pd.DataFrame(result, columns=col_names)
    df = df.iloc[:, 1].tolist()

    for parent_id in df:

        query = "SELECT comment_id from initial_comment where comment_id = '{}'LIMIT 1".format(parent_id)
        c.execute(query)
        result = c.fetchone()
        #print(result)
        if result == None:
            query = "DELETE from replies WHERE parent_id = '{}'".format(parent_id)
            c.execute(query)
            con.commit()



if __name__ == '__main__':
    
    remove_childless_parents()
    print('database initial_comments cleaned')
    remove_possible_duplicates()
    print('database replies cleaned')
    
    con.close()



