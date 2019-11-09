import psycopg2
import json
import re
import os


#establish connection
con = psycopg2.connect(
    database='nlp_db',
    password='password')

c = con.cursor()


def create_table():
    c.execute('CREATE TABLE IF NOT EXISTS initial_comment(comment_id TEXT PRIMARY KEY, parent_id TEXT, link_id TEXT, comment TEXT, subreddit TEXT, utc INT, score INT)')
    c.execute('CREATE TABLE IF NOT EXISTS replies(comment_id TEXT UNIQUE, parent_id TEXT, link_id TEXT, comment TEXT, subreddit TEXT, utc INT, score INT)')
    con.commit()

def format_data(data):
    if not deleted(data):
        if len(data) > 15 or len(data.split(' ')) < 150:
            if "[removed]" not in data:
                if data is not '':
                    if "[deleted]" not in data:
                        if 'http://' not in data:
                            if '\n' or '\r' or "'" in data:
                                #need to replace once added to db
                                data = data.replace("'", '"')
                                #special characters
                                pattern = r'[^a-zA-Z0-9\s]'
                                data = re.sub(pattern, '', data)
                                #lowercase
                                data = data.lower()
                                #new lines
                                data = data.replace(
                                    "\n", "...")
                                #new lines
                                data = data.replace(
                                    "\r", "...")
                                #&gt
                                data = data.replace(" gt ", "")
                                #excessive spacing
                                data = re.sub("\s\s+", " ", data)
                                #filter data
                                with open('./filter_lists/redacted.txt', buffering=1000) as f:
                                    for row in f:
                                        word = row.replace('\n', '')
                                
                                        if word in data:
                                            data = data.replace(word, ' *censored* ')
                                            #print(data)
                                            return data


                                
                         

def deleted(data):
    if '[deleted]' in data:
        return True

def filter_subreddit(subreddit):
    #print(row['subreddit'])
    if ((row['subreddit'] == 'relationship_adivce')or (row['subreddit'] == 'relationships')
        or (row['subreddit'] == 'Advice') or (row['subreddit'] == 'CasualConversation')
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
        or (row['subreddit'] == 'CatFacts') or (row['subreddit'] == 'thebestoflegaladvice') 
        or (row['subreddit'] == 'getmotivated') or (row['subreddit'] == 'wholesome')
        or (row['subreddit'] == 'unexpected') or (row['subreddit'] == 'wholesomeneighbors')
        or (row['subreddit'] == 'aww') or (row['subreddit'] == 'awweducational')
        or (row['subreddit'] == 'unexpectedlywholesome') or (row['subreddit'] == 'mademesmile')
        or (row['subreddit'] == 'eyebleach') or (row['subreddit'] == 'productivity') 
        or (row['subreddit'] == 'lifeprotips') or (row['subreddit'] == 'AdviceAnimals') 
        or (row['subreddit'] == 'getStudying') or (row['subreddit'] == 'Gardening') 
        or (row['subreddit'] == 'cats') or (row['subreddit'] == 'NonZeroDay') 
        or (row['subreddit'] == 'getmotivated') or (row['subreddit'] == 'ZenHabits') 
        or (row['subreddit'] == 'Meditation') or (row['subreddit'] == 'randomactsofkindness')
        or (row['subreddit'] == 'UpliftingNews') or (row['subreddit'] == 'HumansBeingBros')
        or (row['subreddit'] == 'startledcats') or (row['subreddit'] == 'adorableart') 
        or (row['subreddit'] == 'contagiouslaughter') or (row['subreddit'] == 'FeelsLikeTheFirstTime')
        or (row['subreddit'] == 'accidentalComedy')or (row['subreddit'] == 'ActLikeYouBelong')
        or (row['subreddit'] == 'AnimalsBeingDerps')or (row['subreddit'] == 'animalsdoingstuff')
        or (row['subreddit'] == 'Animals')or (row['subreddit'] == 'AskAcademia')
        or (row['subreddit'] == 'AskAnAmerican')or (row['subreddit'] == 'AskAnthropology')
        or (row['subreddit'] == 'AskCulinary')or (row['subreddit'] == 'AskDocs')
        or (row['subreddit'] == 'AskHistorians')or (row['subreddit'] == 'askgaybros')
        or (row['subreddit'] == 'askphilosophy')or (row['subreddit'] == 'AskReddit')
        or (row['subreddit'] == 'awesome')or (row['subreddit'] == 'babybigcatgifs')
        or (row['subreddit'] == 'beauty')or (row['subreddit'] == 'bestof')
        or (row['subreddit'] == 'bestoflegaladvice')or (row['subreddit'] == 'legaladvice')
        or (row['subreddit'] == 'blackcats')or (row['subreddit'] == 'dadjokes')
        or (row['subreddit'] == 'daddit')or (row['subreddit'] == 'dadrelfexes')
        or (row['subreddit'] == 'damnthatsinteresting')or (row['subreddit'] == 'dankchristianmemes')
        or (row['subreddit'] == 'dankmemes')or (row['subreddit'] == 'dating')
        or (row['subreddit'] == 'dating_advice')or (row['subreddit'] == 'datingoverthirty')
        or (row['subreddit'] == 'IAmA')or (row['subreddit'] == 'doggos')or (row['subreddit'] == 'dogtraining')
        or (row['subreddit'] == 'dontflinch')or (row['subreddit'] == 'femalefashionadvice')
        or (row['subreddit'] == 'Feminism')or (row['subreddit'] == 'fancyfollicles')
        or (row['subreddit'] == 'FellowKids')or (row['subreddit'] == 'financialindependence')
        or (row['subreddit'] == 'findapath')or (row['subreddit'] == 'FinancialPlanning')
        or (row['subreddit'] == 'fitmeals')or (row['subreddit'] == 'Fitness')
        or (row['subreddit'] == 'ForeverAloneDating')or (row['subreddit'] == 'Frugal')
        or (row['subreddit'] == 'FrugalFemaleFashion')or (row['subreddit'] == 'gardening')
        or (row['subreddit'] == 'GetStudying')or (row['subreddit'] == 'happy')
        or (row['subreddit'] == 'happycowgifs')or (row['subreddit'] == 'happycrowds')
        or (row['subreddit'] == 'Heavymind')or (row['subreddit'] == 'hiphopheads')
        or (row['subreddit'] == 'history')or (row['subreddit'] == 'explainlikeimfive')
        or (row['subreddit'] == 'holdmycatnip')or (row['subreddit'] == 'HomeworkHelp')
        or (row['subreddit'] == 'howto')or (row['subreddit'] == 'humblebrag')
        or (row['subreddit'] == 'humor')or (row['subreddit'] == 'IndoorGarden')
        or (row['subreddit'] == 'JusticeServed')or (row['subreddit'] == 'nature')
        or (row['subreddit'] == 'NegativeWithGold')or (row['subreddit'] == 'nottheonion')
        or (row['subreddit'] == 'OkCupid')or (row['subreddit'] == 'offmychest')
        or (row['subreddit'] == 'offbeat')or (row['subreddit'] == 'OldSchoolCool')
        or (row['subreddit'] == 'needadvice')or (row['subreddit'] == 'Needafriend')
        or (row['subreddit'] == 'recipes')or (row['subreddit'] == 'raining')
        or (row['subreddit'] == 'puns')or (row['subreddit'] == 'pugs')
        or (row['subreddit'] == 'askscience')or (row['subreddit'] == 'stopdrinking')
        or (row['subreddit'] == 'stopsmoking')or (row['subreddit'] == 'SuicideWatch')
        or (row['subreddit'] == 'TooAfraidToAsk')or (row['subreddit'] == 'todayileanted')
        or (row['subreddit'] == 'TrueAskReddit')or (row['subreddit'] == 'whatcouldgoright')
        or (row['subreddit'] == 'wholesomegrrentext')or (row['subreddit'] == 'WholesomeComics')
        or (row['subreddit'] == 'WritingPrompts')or (row['subreddit'] == 'yesyesyesyesno')or (row['subreddit'] == 'relationships')
        or (row['subreddit'] == 'confession')or (row['subreddit'] == 'relationship_advice')or (row['subreddit'] == 'dating_advice')or (row['subreddit'] == 'LongDistance')
        or (row['subreddit'] == 'ForeverAlone')or (row['subreddit'] == 'actuallesbians')or (row['subreddit'] == 'Showerthoughts')or (row['subreddit'] == 'MGTOW')or (row['subreddit'] == 'dating')or (row['subreddit'] == 'BreakUps')or (row['subreddit'] == 'confessions')or (row['subreddit'] == 'niceguys')
        or (row['subreddit'] == 'teenagers')or (row['subreddit'] == 'raisedbynarcissists')or (row['subreddit'] == 'funny')or (row['subreddit'] == 'LGBTeens')or (row['subreddit'] == 'unpopularopinion')or (row['subreddit'] == 'lgbt')or (row['subreddit'] == 'childfree')or (row['subreddit'] == 'science')
        or (row['subreddit'] == 'JUSTNOMIL')or (row['subreddit'] == 'asexuality')or (row['subreddit'] == 'asktransgender')or (row['subreddit'] == 'casualiama')or (row['subreddit'] == 'Jokes')or (row['subreddit'] == 'psychology')
        or (row['subreddit'] == 'socialskills')or (row['subreddit'] == 'ExNoContact')or (row['subreddit'] == 'Parenting')or (row['subreddit'] == 'memes')
        or (row['subreddit'] == 'AskWomenOver30')or (row['subreddit'] == 'NoStupidQuestions')or (row['subreddit'] == 'changemyview')or (row['subreddit'] == 'survivinginfidelity')or (row['subreddit'] == 'bestof')or (row['subreddit'] == 'tipofmytongue')
        or (row['subreddit'] == 'wowthanksimcured')):
        return subreddit
    else:
        return None


#db functions
def check_if_updated(comment_id, parent_id, link_id, comment, subreddit, utc, score):
    query = "SELECT link_id FROM replies WHERE link_id = '{}' LIMIT 1".format(
        link_id)
    c.execute(query)
    result = c.fetchone()

    if result != None:
        update_reply(comment_id, parent_id, link_id,
                     comment, subreddit, utc, score)
    else:
        #print(('inserting {} into replies').format(comment_id))
        query3 = """INSERT INTO replies(comment_id, parent_id, link_id, comment, subreddit, utc, score) VALUES ('{}','{}', '{}', '{}','{}',{},{});""".format(
            comment_id, parent_id, link_id, comment, subreddit, utc, score)
        c.execute(query3)
        con.commit()

def update_reply(comment_id, parent_id, link_id, comment, subreddit, utc, score):
    #no linked comments
    query = "SELECT link_id FROM replies WHERE link_id = '{}' LIMIT 1".format(
        link_id)

    c.execute(query)
    result = c.fetchone()
    #print(result)

    if result != None:
        #print(('{} has an initial reply already').format(comment_id))
        try:
            reply_score = "SELECT score FROM replies WHERE link_id = '{}' LIMIT 1 ".format(
                link_id)

            c.execute(reply_score)
            reply_in_db = c.fetchone()
            reply_in_db = int(reply_in_db[0])

            if score > reply_in_db:
               # print(('updating {} reply based on score').format(comment_id))
                query = """UPDATE replies SET comment_id = '{}', parent_id = '{}', link_id = '{}', comment = '{}', subreddit = '{}', utc = '{}', score = '{}' WHERE comment_id = '{}'""".format(
                    comment_id, parent_id, link_id, comment, subreddit, utc, score, comment_id)
                c.execute(query)
                con.commit()
               # print('updated successfully')

        except Exception as e:
            raise e
           # print('comment could not be updated')

def check_if_parent(comment_id, parent_id, link_id, comment, subreddit, utc, score):
            if parent_id == link_id:
               #print(('{} is the root comment').format(comment_id))

                insert_parent(comment_id, parent_id, link_id,
                              comment, subreddit, utc, score)
            if parent_id != link_id:
                check_if_updated(comment_id, parent_id, link_id,
                                 comment, subreddit, utc, score)

def insert_parent(comment_id, parent_id, link_id, comment, subreddit, utc, score):
    query2 = """INSERT INTO initial_comment(comment_id, parent_id, link_id, comment, subreddit, utc, score) VALUES ('{}','{}', '{}', '{}','{}',{},{});""".format(
        comment_id, parent_id, link_id, comment, subreddit, utc, score)
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
    query = "SELECT comment_id FROM replies WHERE link_id = '{}' LIMIT 1".format(
        linkid)
    c.execute(query)
    result = c.fetchone()
    if result is not None:
        return result[0]
    else:
        return False



def close_connection():
    con.close()



if __name__ == '__main__':
    row_counter = 0

   
    path_name = "../data/"
    cwd = os.getcwd()
    files = os.listdir(cwd)
    #print("Files in %r: %s" % (cwd, files))
    data = os.path.abspath("../data/")


for file in os.listdir(data):
        filename = os.fsdecode(file)
        try:
            #line = db.replace('\n', '')
            #print(line)
            with open(data+'/'+filename, buffering=1000) as f:
                print(data+'/'+filename)
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
                    #print(subreddit)
                    #subreddit = (row['subreddit'])
                    if score >= 10 and (subreddit is not None) and (comment is not None):
                            #print(subreddit)
                            try:
                                check_if_parent(
                                    comment_id, parent_id, link_id, comment, subreddit, utc, score)
                                has_parent_comment = find_parent(parent_id)
                                if has_parent_comment:
                                    update_reply(
                                        comment_id, parent_id, link_id, comment, subreddit, utc, score)
                                    #print('reply exists, updating if needed')
                            except Exception as e:
                                print(str(e))

        except Exception as e:
            raise e
            print('Filelist.txt did not have any files to go through')
            close_connection()
