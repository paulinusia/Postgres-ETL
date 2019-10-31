import psycopg2
import json
import re
import os




#establish connection
con = psycopg2.connect(
        database= 'db2008',
        password= 'password')

c = con.cursor()

def create_table():
    c.execute('CREATE TABLE IF NOT EXISTS initial_comment(comment_id TEXT PRIMARY KEY, parent_id TEXT, link_id TEXT, comment TEXT, subreddit TEXT, utc INT, score INT)')
    c.execute('CREATE TABLE IF NOT EXISTS replies(comment_id TEXT UNIQUE, parent_id TEXT, link_id TEXT, comment TEXT, subreddit TEXT, utc INT, score INT)')
    con.commit()

def format_data(data):
    if not deleted(data):
            if len(data) > 10 or len(data.split(' ')) < 500:
                        if "[removed]" not in data:
                            if data is not '':
                                if "[deleted]" not in data:
                                    if 'http://' not in data:
                                        if '\n' or '\r' or "'" in data:
                                            #need to replace once added to db
                                            data = data.replace("'", '"')
                                            #special characters
                                            pattern = r'[^a-zA-Z0-9\s]'
                                            data =re.sub(pattern,'',data)
                                            #new lines
                                            data = data.replace("\n", " NEWLINE ")
                                            #new lines
                                            data = data.replace("\r", " NEWLINE ")
                                            #excessive spacing
                                            data = re.sub("\s\s+", " ", data)

                                            return data


def deleted(data):
    if '[deleted]' in data:
        return True

def filter_subreddit(subreddit):
    if ((row['subreddit'] == 'relationships') or (row['subreddit'] == 'Advice')
        or (row['subreddit'] == 'relationshipadvice') or (row['subreddit'] == 'relationship_advice')
        or (row['subreddit'] == 'askwomenadvice') or (row['subreddit'] == 'dating_advice')
        or (row['subreddit'] == 'AskMen') 
        or (row['subreddit'] == 'AskWomen') or (row['subreddit'] == 'getdisciplined')
        or (row['subreddit'] == 'Parenting') or (row['subreddit'] == 'Divorce')
       or (row['subreddit'] == 'mentalhealth')
        or (row['subreddit'] == 'Anxiety') or (row['subreddit'] == 'depression')
        or (row['subreddit'] == 'ADHD') or (row['subreddit'] == 'offmychest')
        or (row['subreddit'] == 'confessions') or (row['subreddit'] == 'rant')
        or (row['subreddit'] == 'CasualConversation') or (row['subreddit'] == 'confession')
        or (row['subreddit'] == 'NoStupidQuestions') 
        or (row['subreddit'] == 'TrollXSupport') or (row['subreddit'] == 'breakingmom')
       or (row['subreddit'] == 'CatFacts') or (row['subreddit'] == 'thebestoflegaladvice') or (row['subreddit'] == 'getmotivated')):
        
        return subreddit

    else:
        return None


def check_if_updated(comment_id, parent_id, link_id, comment, subreddit, utc, score):
    query = "SELECT link_id FROM replies WHERE link_id = '{}' LIMIT 1".format(link_id)
    c.execute(query)
    result = c.fetchone()

    if result != None:
        update_reply(comment_id, parent_id, link_id, comment, subreddit, utc, score)
    else:
        print(('inserting {} into replies').format(comment_id))
        query3 = """INSERT INTO replies(comment_id, parent_id, link_id, comment, subreddit, utc, score) VALUES ('{}','{}', '{}', '{}','{}',{},{});""".format(comment_id, parent_id, link_id, comment, subreddit, utc, score)
        c.execute(query3)
        con.commit()


def update_reply(comment_id, parent_id, link_id, comment, subreddit, utc, score):
    #no linked comments
    query = "SELECT link_id FROM replies WHERE link_id = '{}' LIMIT 1".format(link_id)

    c.execute(query)
    result = c.fetchone()
    print(result)


    if result != None:
        print(('{} has an initial reply already').format(comment_id))
        try: 
            reply_score = "SELECT score FROM replies WHERE link_id = '{}' LIMIT 1 ".format(link_id)

            c.execute(reply_score)
            reply_in_db = c.fetchone()
            reply_in_db = int(reply_in_db[0])

       

            if score > reply_in_db:
                print(('updating {} reply based on score').format(comment_id))
                query = """UPDATE replies SET comment_id = '{}', parent_id = '{}', link_id = '{}', comment = '{}', subreddit = '{}', utc = '{}', score = '{}' WHERE comment_id = '{}'""".format(comment_id, parent_id, link_id, comment, subreddit, utc, score, comment_id)
                c.execute(query)
                con.commit()
                print('updated successfully')
         
        except Exception as e:
            raise e
            print('comment could not be updated')





def check_if_parent(comment_id, parent_id, link_id, comment, subreddit, utc, score):
            if parent_id == link_id:
                print(('{} is the root comment').format(comment_id))
                insert_parent(comment_id, parent_id, link_id, comment, subreddit, utc, score)
            if parent_id != link_id: 
                check_if_updated(comment_id, parent_id, link_id, comment, subreddit, utc, score)
                


#helper functions
def insert_parent(comment_id, parent_id, link_id, comment, subreddit, utc, score):
    query2 = """INSERT INTO initial_comment(comment_id, parent_id, link_id, comment, subreddit, utc, score) VALUES ('{}','{}', '{}', '{}','{}',{},{});""".format(comment_id, parent_id, link_id, comment, subreddit, utc, score)
    c.execute(query2)
    con.commit()

def find_parent(parent):
    query = "SELECT comment_id FROM initial_comment WHERE comment_id= '{}' LIMIT 1".format(
        parent)
    c.execute(query)
    result = c.fetchone()  # db will only fetch one value from the parent db
    if result is not None:
        #return result[comment_id]
        return result[0]
    else:
        return False

def find_linked_comment(linkid):
    query = "SELECT comment_id FROM replies WHERE link_id = '{}' LIMIT 1".format(linkid)
    c.execute(query)
    result = c.fetchone()
    if result is not None:
        return result[0]
    else:
        return False
'''
TODO:

ADD METHODS

    FILTER EXPLICIT LANGUAGE BY REPLACING CURSE WORDS WITH 'REDACTED'
    FILTER EXPLICIT SUBJECTS... DISREGARD COMMENT COMPLETELY



    REMOVE EXCESSIVE SPACING IN COMMENTS
    REMOVE EXCESSIVE NEW LINE COMMENTS
    REMOVE SPECIAL CHARACTERS
    CASE CONVERSIONS
    REMOVAL OF STOP WORDS

    EVENTUALLY REPLACE ALL " WITH '

    IMPLEMENT THE NLTK LIBRARY



'''

def close_connection():
    con.close();


'''
TODO:

ADD TO BLACKBOX LOCATION

FIX FILE STRUCTURE


'''
if __name__ == '__main__':
    row_counter = 0

    '''
    TODO:

    FILTER THROUGH FILES/AUTOMATICALLY JUMP FROM FILE TO FILE

    AUTOMATICALLY SWITCH DATABASES ONCE FILTERED THROUGH THE FILES... IS THIS AN OPTION

    NOTES
        COMMENTS LOAD INTO DB WHEN FILES ARE NOT FILTERED THROUGH AND THE CONNECTION IS CLOSED AT THE END

    '''
    path_name = "../data/"
    cwd = os.getcwd()
    files = os.listdir(cwd)
    print("Files in %r: %s" % (cwd, files))
    data  = os.path.abspath("../data/")


for file in os.listdir(data):
        filename = os.fsdecode(file)
        try:
            #line = db.replace('\n', '')
            #print(line)
            with open(data+'/'+filename, buffering=1000) as f:
                #print(data+line)
                create_table()
                for row in f:
                    
                    row = json.loads(row)
                    row_counter += 1
                    
                    #print(row)
                    comment_id = row['name']
                    #print(comment_id)
                    parent_id = row['parent_id']
                    #print(parent_id)
                    link_id = row['link_id']
                    #print(link_id)
                    comment = format_data(row['body'])
                    #print(comment)
                    utc = row['created_utc']
                    #print(utc)
                    score = int(row['score'])

                    #print('score type initiated: ', type(score))
                    #print(score)
                    subreddit = filter_subreddit(row['subreddit'])

                    if score >= 10 and (subreddit is not None) and (comment is not None):
                        

                            #print(subreddit)
                        try:
                            check_if_parent(comment_id, parent_id, link_id, comment, subreddit, utc, score)
                            has_parent_comment = find_parent(parent_id)
                            if has_parent_comment:

                                '''
                                TODO:

                                Fix update/replace comment. Currently adds all replies to DB, does not replace/upate any 

                                COULD BE ISSUE WITH CURRENT REPLY SCORE
                                WHAT HAPPENS IF SCORES ARE IDENTIAL
                                ISSUE WITH DB?
                                '''
                                update_reply(comment_id, parent_id, link_id, comment, subreddit, utc, score)
                                #print('reply exists, updating if needed')
                        except Exception as e:
                            print(str(e))



        except Exception as e:
            raise e
            print('Filelist.txt did not have any files to go through')
            close_connection()


