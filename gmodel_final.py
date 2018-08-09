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
    
