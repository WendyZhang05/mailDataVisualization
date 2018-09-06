import sqlite3
import time
import zlib

number=int(input('how many top ranked email address and organization you want to display:'))

conn=sqlite.connet('index.sqlite')
cur=conn.cursor()
