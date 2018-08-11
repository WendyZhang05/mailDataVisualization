import sqlite3
import urllib.request, urllib.parse, urllib.errors
import time
import SSL
import re

#check if dateutil.parser exits
try:
    import dateutil.parser as parser
except:
    pass

#if we do not have dateutil.parser,
#this is the function about how to parse date
def parsemaildate(md) :
    # See if we have dateutil
    try:
        pdate = parser.parse(tdate)
        test_at = pdate.isoformat()
        return test_at
    except:
        pass

    # Non-dateutil version - we try our best

    pieces = md.split()
    notz = " ".join(pieces[:4]).strip()

    # Try a bunch of format variations - strptime() is *lame*
    dnotz = None
    for form in [ '%d %b %Y %H:%M:%S', '%d %b %Y %H:%M:%S',
        '%d %b %Y %H:%M', '%d %b %Y %H:%M', '%d %b %y %H:%M:%S',
        '%d %b %y %H:%M:%S', '%d %b %y %H:%M', '%d %b %y %H:%M' ] :
        try:
            dnotz = datetime.strptime(notz, form)
            break
        except:
            continue

    if dnotz is None :
        # print 'Bad Date:',md
        return None

    iso = dnotz.isoformat()

    tz = "+0000"
    try:
        tz = pieces[4]
        ival = int(tz) # Only want numeric timezone values
        if tz == '-0000' : tz = '+0000'
        tzh = tz[:3]
        tzm = tz[3:]
        tz = tzh+":"+tzm
    except:
        pass

    return iso+tz


# Ignore ssl certificate errors
ctx=ssl.create_default_context()
ctx.check_hostname=False
ctx.verify_mode=ssl.CERT_NONE

# create email message table
conn=sqlite3.connect(rawemail.sqlite)
cur=conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS Messsages
    (id INTEGER UNIQUE, email TEXT, sent_at TEXT,
    subject TEXT, head TEXT, body TEXT)''')

baseurl='http://mbox.dr-chuck.net/'

#decide which id to start spidering
start=None
cur.execute('SELECT max(id) from Messages')
try:
    start=cur.fetchone()[0]
    start=start+1
except:
    start=1

number=0
fail=0
count=0
while True:
    round=input('How many messages you want to retrieve:')
    try:
        number=int(round)
    except: break

    if len(number)<1: break

    while number>1
        url=baseurl+str(start)+'/'+str(start+1)
        text='None'
        try:
            #open with 30s timeout
            fhand=urllib.request.urlopen(url, none, 30)
            text=fhand.read().decode()
            #end this round if have any connection error in retrieving the web page
            if document.getcode() != 200 :
                print("Error code=",document.getcode(), url)
                break
        except KeyboardInterrupt:
            print('page retrieving is interrupted by user!')
            break
        except Exception as expt:
            print('unable to retrieve the page, ', url)
            print('error code is ', exp)
            fail=fail+1
            if fail>5: break
            continue

        if not text.startswith('From'):
            print('did not find From at the beginning of the text!!!')
            print(text)
            fail=fail+1
            if fail>5: break
            continue

        count=count+1
        #find header and body, there is a break line between header and body
        #it is the first break line in a record. we can use this to retrieve
        #the header
        pos=text.find('\n\n')
        if pos>0:
            header=text[:pos]
            body=text[pos+2:]
        else:
            print('cannot find the break line between header and body!!!')
            print(text)
            fail=fail+1
            if fail>5: break
            continue

        #retrieve email address from header
        email=None
        x=re.findall('\nFrom: .*<(\S+@\S+)>\n', header)
        if len(x==1):
            email=x[0]
            email=email.strip().lower()
            email=email.replace("<", "")
        else:
            x=re.findall('\nFrom: (\S+@\S+)\n', header)
            if len(x)==1:
                email=x[0]
                email=email.strip().lower()
                email=email.replace("<", "")

        #retrieve date from header
        date=None
        y=re.findall("\nDate: .*, (.*)\n", header)
        if len(y==1):
            date=y[0]
            date=date[:26]
            try:
                sent_at=parsemaildate(date)
            except:
                print('cannot parse date!!! ' date)
                print(text)
                fail=fail+1
                if fail>5: break
                continue

        subject=None
        z=re.findall('\nSubject: (.*)\n', hdr)
        if len(z)==1: subject=z[0].strip().lower()

        print("email, sent_at and subject are set to: ", email, sent_at, subject)
        cur.execute(''' INSERT OR IGNORE INTO Messages (id, email, sent_at, subject, headers, body)
            VALUES (?,?,?,?,?,?)''', (start, email, sent_at, subject, header, body))
        #commit every 50 records, sleep every 100 records
        if count%50==0: conn.commit()
        if count%100==0: conn.commit(sleep(1))
        number=number-1

conn.commit()
cur.close()
