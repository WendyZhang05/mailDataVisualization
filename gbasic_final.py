import sqlite3
import time
import zlib

number=int(input('how many top ranked email address and organization you want to display:'))

addresses=dict()
organizations=dict()
count=0
conn=sqlite3.connect('index.sqlite')
cur=conn.cursor()
conn_1=sqlite3.connect('index.sqlite')
cur_1=conn.cursor()

#retrieve email address and organization from database and count their times,
#save email and the times to addresses dictionary, save organization and times
#to organizations dictionary
cur.execute('SELECT sender_id FROM Messages')
for row in cur:
    count=count+1
    email_id=row[0]
    cur_1.execute('SELECT sender FROM Senders WHERE id=? LIMIT 1', (email_id,))
    try:
        row_1=cur_1.fetchone()
        email=row_1[0]
        addresses.setdefault(email, 0)
        addresses[email]=addresses[email]+1
    except:
        print("cannot retrieve address")
        break

    pieces=email.split("@")
    org=pieces[-1:]
    org_name=org[0]
    organizations.setdefault(org_name, 0)
    organizations[org_name]=organizations[org_name]+1

print('loaded messages:', count)

#user sorted function to sort values in decremental order for addresses and organizations dictionaries
sorted_addresses= sorted((value, key) for (key,value) in addresses.items())
sorted_organizations=sorted((value, key) for (key,value) in organizations.items())
# reverse the the lists above
sorted_addresses.sort(reverse=True)
sorted_organizations.sort(reverse=True)

print('top', number, 'email list participants:')
for item in sorted_addresses[0:number]:
    print(item[1],": ", item[0])

print('\n')
print('top', number, 'organization in email list:')
for item in sorted_organizations[0:number]:
    print(item[1],": ", item[0])

cur.close()
