import psycopg2
import json
import re
import os

#establish connection
con = psycopg2.connect(
        database= 'db2010',
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
                                        if '\n' or '\r' in data:
                                            if "'" in data:
                                                data = data.replace("'", '"')
                                                data = data.replace("\n", " NEWLINE ")
                                                data = data.replace("\r", " NEWLINE ")
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
        or (row['subreddit'] == 'NoStupidQuestions') or (row['subreddit'] == 'TrollXChromosomes')
        or (row['subreddit'] == 'TrollXSupport') or (row['subreddit'] == 'breakingmom')
        or (row['subreddit'] == 'TwoXChromosomes') or (row['subreddit'] == 'CatFacts')
        or (row['subreddit'] == 'thebestoflegaladvice') or (row['subreddit'] == 'getmotivated')):
        return subreddit
    else:
        pass



def update_reply(comment_id, parent_id, link_id, comment, subreddit, utc, score):
    #no linked comments
    query = "SELECT link_id FROM replies WHERE link_id = '{}' LIMIT 1".format(link_id)
    c.execute(query)
    result = c.fetchone()
    if result != None:
        print(('{} has an initial reply already').format(comment_id))
        reply_score = "SELECT score FROM replies WHERE link_id = '{}' LIMIT 1 ".format(link_id)
        c.execute(reply_score)
        reply = c.fetchone()
        r = reply[0]
        
       # print(type(r))
       # print(r)
        if score > r:
            print(('updating {} reply based on score').format(comment_id))
            query = """UPDATE replies SET comment_id = '{}', parent_id = '{}', link_id = '{}', comment = '{}', subreddit = '{}', utc = '{}', score = '{}' WHERE comment_id = '{}'""".format(
                comment_id, parent_id, link_id, comment, subreddit, utc, score, comment_id)
            c.execute(query)
            con.commit()
            print('updated successfully')


def update_score(reply_score, score):
    reply_score = int(reply_score, 10)
    score = int(score, 10)

    if score > reply_score:
            print(('updating {} reply based on score').format(comment_id))
            query = """UPDATE replies SET comment_id = ?, parent_id = ?, link_id = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE comment_id =?;""".format(comment_id, parent_id, link_id, comment, subreddit, utc, score, comment_id)
            c.execute(query)
            con.commit()
            print('updated successfully')


def check_if_parent(comment_id, parent_id, link_id, comment, subreddit, utc, score):
            if parent_id == link_id:
                print(('{} is the root comment').format(comment_id))
                insert_parent(comment_id, parent_id, link_id, comment, subreddit, utc, score)
            '''
            query = "SELECT link_id FROM initial_comment WHERE link_id = '{}' LIMIT 1".format(link_id)
            c.execute(query)
            result = c.fetchone()
            query2 = "SELECT parent_id FROM initial_comment WHERE parent_id = '{}' LIMIT 1".format(parent_id)
            c.execute(query2)
            result2 = c.fetchone()
            if result == result2:
                print(('{} is the root comment').format(comment_id))
                insert_parent(comment_id, parent_id, link_id, comment, subreddit, utc, score)
            '''
            if parent_id != link_id: 
                print(('inserting {} into replies').format(comment_id))
                query3 = """INSERT INTO replies(comment_id, parent_id, link_id, comment, subreddit, utc, score) VALUES ('{}','{}', '{}', '{}','{}',{},{});""".format(comment_id, parent_id, link_id, comment, subreddit, utc, score)
                c.execute(query3)
                con.commit()


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

def close_connection():
    con.close();

if __name__ == '__main__':
    row_counter = 0
    
    files_path = '../data/'
    current_file = '2010-05'


with open('filelist.txt') as filenames:
    for row in filenames:
        name = row.replace("\n", "")

        with open(files_path + name, buffering=1000) as f:
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


                if score >= 10 and (subreddit and comment is not None):
                    
                        #print(subreddit)
                    try:
                        check_if_parent(comment_id, parent_id, link_id, comment, subreddit, utc, score)
                        has_parent_comment = find_parent(parent_id)
                        if has_parent_comment:
                            update_reply(comment_id, parent_id, link_id, comment, subreddit, utc, score)
                            #print('reply exists, updating if needed')
                    except Exception as e:
                        print(str(e))

            close_connection()

            
