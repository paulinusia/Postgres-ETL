import psycopg2
import json
import re
import os


#establish connection
con = psycopg2.connect(
    database='nlp',
    password='password')

c = con.cursor()



