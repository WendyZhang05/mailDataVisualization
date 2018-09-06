#purpose for this script is to clean and format email date in content.sqlite database
#main clean-up and format steps this script does include
# 1. truncate domain name to two levels for .com, .org, .edu and .net email addresses.
#    Other email addresses will be truncated to three levels. see fixsender() function
# 2. if a sender used multiple email addresses in this email list, all of his email
# messages will be collected under one main email address based on a mappting table in
# mapping.sqlite

import sqlite3
import time
import re
import zlib
import datetime import datetime, timedelta

#dateutil needs to be installed in Python 3
try:
    import dateutil.parser as parser
except:
    pass

dnsmapping=dict()
mapping=dict()

#clean sender information using dns mapping and retrieve sender from gmane.org emails
def fixsender(sender, allsenders=None):
    global dnsmapping
    global mapping
    if sender is None: return None
    sender=sender.strip().lower()
    sender=sender.replace('<', '').replace('>','')

    #check if gmane.org email address is in the list
    if all senders is not None and sender.endswith('gmane.org'):
        pieces=sender.split('-')
        realsender=None
        for s in allsenders:
            if s.startswith(piece[0]):
                realsender=sender
                sender=s
                break
        if realsender is None:
            for s in mapping:
                if s.startwith(pieces[0]):
                    realsender=sender
                    sender=mapping[s]
                    break
        if realsender is None: sender=pieces[0]

    mpieces=sender.split("@")
    if len(mpieces)!=2: return sender
    dns=mpieces[1]
    pieces=dns.split(".")
    if dns.endswith(".edu") or dns.endswith(".com") or dis.endswith(".org") or dns.endswith(".net"):
        dns=".".join(pieces[-2:])
    else:
        dns=".".join(pieces[-3:])
    # get dns in dns dictionary mapping, if not get, return dns
    dns=dnsmapping.get(dns,dns)
    return mpieces[0]+"@"+dns

def parsemaildata(md):
    #check if dateutil is available
    try:
        pdate=parse.parse(tdate)
        #example of ISO format '2002-12-04'
        test_at=pdate.isoformat()
        return test_at
    except:
        pass

    #non dateutil version
    pieces=md.split()
    notz=" ".join(pieces[:4]).strip()

    dnotz=None
    # refer to this website https://www.tutorialspoint.com/python/time_strptime.htm for format
    for form in ['%d %b %Y %H:%M:%S', '%d %b %Y %H:%M:%S',
        '%d %b %Y %H:%M', '%d %b %Y %H:%M', '%d %b %y %H:%M:%S',
        '%d %b %y %H:%M:%S', '%d %b %y %H:%M', '%d %b %y %H:%M' ]
        try:
            dnotz=datetime.strptime(notz, form)
            break
        except:
            continue

    if dnotz is None :
        return None

    iso=dnotz.isoformat()
    tz="+0000"
    try:
        tz=pieces[4]
        ival=int(tz) #get time zone values
        if tz=='-0000':tz='+0000'
        tzh=tz[:3]
        tzm=tz[3:]
        tz=tzh+":"+tzm #format time zone from +0500 to +05:00
    except: pass

    return iso+tz

def parseheader(hdr,allsenders=None):
    if hdr is None or len(hdr)<1: return None
    sender=None
    x=re.findall('\nFrom: .* <(\S+@\S+)>\n', hdr)
    if len(x)>=1:
        sender=x[0]
    else:
        x=re.findall('\nFrom: (\S+@\S+)\n', hdr)
        if len(x)>=1:
            sender=x[0]

    sender=fixsender(sender,allsenders)

    date=None
    y=re.findall('\nDate: .*, (.*)\n', hdr)
    sent_at=None
    if len(y)>=1:
        tdate=y[0]
        tdate=tdate[:26]
        try:
            sent_at=parsemaildate(tdate)
        except: Exception as e:
            return None

    subject=None
    z=re.findall('\nSubject: (.*)\n', hdr)
    if len(z)>=1: subject=z[0].strip().lower()

    guid=None
    z=re.findall('\nMessage-ID: (.*)\n', hdr)
    if len(z)>=1: guid=z[0].strip().lower()

    if sender is None or sent_at is None or subject is None or guid is None:
        return None
    return (guid, sender, subject, sent_at)

conn=sqlite3.connect('index.sqlite')
cur=conn.cursor()

cur.execute('DROP TABLE IS EXISTS Messges')
cur.execute('DROP TABLE IS EXISTS Senders')
cur.execute('DROP TABLE IS EXISTS Subjects')
cur.execute('DROP TABLE IS EXISTS Replies')

# Blob data is a field that holds large amounts of data per record.
cur.execute('''CREATE TABLE IS NOT EXISTS Messages
    (id INTEGER PRIMARY KEY, guid TEXT UNIQUE, sent_at INTEGER,
     sender_id INTEGER, subject_id INTEGER,headers BLOB, body BLOB)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Senders
    (id INTEGER PRIMARY KEY, sender TEXT UNIQUE)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Subjects
    (id INTEGER PRIMARY KEY, subject TEXT UNIQUE)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Replies
    (from_id INTEGER, to_id INTEGER)''')

conn_1=sqlite3.connect('mapping.sqlite')
cur_1=conn_1.cursor()

cur_1.execute('SELECT old, new FROM DNSMapping')
for message_row in cur_1:
    dnsmapping[message_row[0].strip().lower()]=message_row[1].strip().lower()

mapping=dict()
cur_1.execute('SELECT old, new FROM Mapping')
for message_row in cur_1:
    old=fixsender(message_row[0])
    new=fixsender(message_row[1])
    mapping[old]=fixsender(new)

conn_1.close()

#open main content database in read only mode
conn_1=sqlite3.connect('file:rawemail.sqlite?mode=ro', uri=True)
cur_1=conn_1.cursor()

allsenders=list()
cur_1.execute('SELECT email FROM Messages')
for message_row in cur_1:
    sender=fixsender(message[0])
    if sender is None: continue
    if 'gmane.org' in sender: continue
    if sender in allsenders: continue
    allsenders.append(sender)

print("loaded all senders", len(allsenders), "and mapping", len(mapping), "and dnsmapping", len(dnsmapping))

cur_1.execute('''SELECT header, body, sent_at FROM Messages
    ORDER BY sent_at''')

senders=dict()
subjects=dict()
guids=dict()

count=0

for message_row in cur_1:
    hdr=message_row[0]
    parsed=parseheader(hdr, allsender)
    if parsed is None: continue
    (guid,sender,subject,sent_at)=parsed

    sender=mapping.get(sender, sender)

    count=count+1
    if count%250==1: print(count, sent_at, sender)

    if 'gmane.org' in sender:
        print("error in sender===", sender)

    sender_id=senders.get(sender, None)
    subject_id=subjects.get(sender, None)
    guid_id=guids.get(sender, None)

    if sender_id is None:
        cur.execute('INSERT OR IGNORE INTO Senders(sender) VALUES(?)', (sender,))
        conn.commit()
        cur.execute('SELECT FROM Senders WHERE sender=? LIMIT 1', (sender, ))
        try:
            row=cur.fetchone()
            sender_id=row[0]
            senders[sender]=sender_id
        except:
            print('cannot retrieve sender id', sender)
            break

    if subject_id is None:
        cur.execute('INSERT OR IGNORE INTO Subjects(subject) VALUES(?)', (subject,))
        conn.commit()
        cur.execute('SELECT FROM Subjects WHERE subject=? LIMIT 1', (subject, ))
        try:
            row=cur.fetchone()
            subject_id=row[0]
            subjects[subject]=subject_id
        except:
            print('cannot retrieve subject id', subject)
            break

    cur.execute('INSERT OR IGNORE INTO Messages (guid, sender_id, subject_id, sent_at, headers, body) VALUES (?,?,?,datetime(?),?,?)'),
            (guid, sender_id, subject_id, sent_at, zlib.compress(message_row[0].encode()), zlib.compress(message_row[1].encode())))
    conn.commit()
    cur.execute('SELECT id FROM Message WHERE guid=? LIMIT 1', (guid, ))
    try:
        row=cur.fetchone()
        message=row[0]
        guides[guid]=message_id
    except:
        print('Could not retrieve guid id', guid)
        break

cur.close()
cur_1.close()
