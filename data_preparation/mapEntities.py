from collections import defaultdict
import re
import urllib2
import csv
import sqlite3

from extractIds import *

def regexp(expr, item):
    if item is None:
        item = ''
    return re.search(expr, item, re.I) is not None

#name the database
dbPath = '../enron.db'
con = sqlite3.connect(dbPath)
con.text_factory = str

# Allow sqlite3 to use regular expressions
con.create_function("REGEXP", 2, regexp)
c = con.cursor()

c.execute("""  
SELECT DISTINCT Xto
FROM rawMessages where Xto REGEXP '\w'
""")
    
recipients_list = c.fetchall()

c.execute("""  
SELECT DISTINCT Xcc
FROM rawMessages where Xcc REGEXP '\w' 
""")

recipients_list.extend(c.fetchall())

recipients = set()
for recipients_tuple in recipients_list:
    recipients.update(parsePluralEntities(recipients_tuple[0]))

c.execute("""  
SELECT DISTINCT Xfrom
FROM rawMessages
""")

senders = c.fetchall()

c.close()

senders = set([sender[0] for sender in senders])

entities = recipients.union(senders)

###
# run through the entities once, indexing the five 'data' fields,
# also create a 'master' list of valid enronIds
###

## assume `entities` is a set with every entity as it occurs
## in the various address fields ('To', 'XFrom', 'Bcc', etc)
ents = list(entities)

dataMatches = {}
for k in ['emailId', 'middle', 'outsideEmail', 'last', 'first']:
    dataMatches[k] = {}
enronIdData = {}

for i,e in enumerate(ents):
    eData = extractId(e)
    # update dataMatches
    for k,v in eData.items():
        for vv in v:
            dataMatches[k].setdefault(vv,set()).add(i)
    #update enronIdData
    for id in eData['emailId']:
        if id not in enronIdData:
            enronIdData[id] = {}
        for k in eData.keys():
            enronIdData[id].setdefault(k,set()).update(eData[k])


# identify people that got multiple emailIds
multiIds = {}
for k,v in enronIdData.iteritems():
    if(len(v['emailId'])>1):
        multiIds[k]=v['emailId']

# it looks like all the multi-id problems are related to "imceanotes" problems
# I'll just fix that here (a little clunky but not a problem):
for k,v in enronIdData.iteritems():
    v['emailId'] = set(filter(lambda id: id[:11]!='imceanotes-',v['emailId']))

# enronIdData gives us 9917 unique emailIds. That's probably too many, but not grossly.


###
# Now run through the valid enronIds, mapping each to some set of entity strings.
###

idEntMap = {}
for enronId,data in enronIdData.iteritems():
    idEntMap[enronId] = set()
    sureMap = False
    # most obviously, anything with the same emailId is a clean map
    for id in enronIdData[enronId]['emailId']:
        idEntMap[enronId].update(dataMatches['emailId'].get(id,set()))
    # we'll also assume that anyone with the same outsideEmail is the same person
    for eml in enronIdData[enronId]['outsideEmail']:
        idEntMap[enronId].update(dataMatches['outsideEmail'].get(eml,set()))
    # then some of the stickier stuff dealing with names.
    # we need to use every combination of first > last > middle
    match=set()
    for fn in  enronIdData[enronId]['first']:
        for ln in  enronIdData[enronId]['last']:
            if len(enronIdData[enronId]['middle'])>0:
                for mn in  enronIdData[enronId]['middle']:

                    # full match on "first middle last"
                    match.update(set.intersection(
                        dataMatches['first'].get(fn,set()),
                        dataMatches['middle'].get(mn,set()),
                        dataMatches['last'].get(ln,set())
                    ))
                    # full match on "first m last"
                    match.update(set.intersection(
                        dataMatches['first'].get(fn,set()),
                        dataMatches['middle'].get(mn[0],set()),
                        dataMatches['last'].get(ln,set())
                    ))
                    # full match on "f middle last"
                    match.update(set.intersection(
                        dataMatches['first'].get(fn[0],set()),
                        dataMatches['middle'].get(mn,set()),
                        dataMatches['last'].get(ln,set())
                    ))
                    # full match on "f m last"
                    match.update(set.intersection(
                        dataMatches['first'].get(fn[0],set()),
                        dataMatches['middle'].get(mn[0],set()),
                        dataMatches['last'].get(ln,set())
                    ))
            else:
                # no middle names
                # full match on "first last"
                match.update(set.intersection(
                    dataMatches['first'].get(fn,set()),
                    dataMatches['last'].get(ln,set())
                ))
                #idEntMap[enronId].update(match)
    # add those that aren't already determined:
    for m in match:
        if not extractId(ents[m])['emailId']:
            idEntMap[enronId].add(m)

# find the conflicts
entIdMap = {}
for id, entSet in idEntMap.iteritems():
    for eId in entSet:
        ent = ents[eId].lower().strip()
        ent_d = extractId(ent)
        entIdMap.setdefault(ent,set()).add(id)

entConflicts = {}
for ent,ids in entIdMap.iteritems():
    if len(ids) > 1:
        entConflicts[ent] = []
        for id in ids:
            entConflicts[ent] += [enronIdData[id]]

def unset(s,sep=',') :
    if not s:
        return ''
    s = list(s)
    if len(s) == 1 :
        return s[0]
    else :
        return sep.join(s)

with open('entityConflicts.csv','w') as f :
    writer = csv.writer(f, dialect="excel")
    writer.writerow(["entityString", "resolvedId", "emailId", "first", "middle", "last", "outsideEmail"])
    for ent in sorted(entConflicts.keys()) :
        for id_info in entConflicts[ent] :
            writer.writerow([ent,
                             '',
                             unset(id_info['emailId']),
                             unset(id_info['first']),
                             unset(id_info['middle']),
                             unset(id_info['last']),
                             unset(id_info['outsideEmail'])
                        ]
                       )

# assuming that the resolved conflicts have been uploaded to google docs,
# map what we couldn't before.

resolvedEntitiesUrl='https://docs.google.com/spreadsheet/pub?hl=en_US&hl=en_US&key=0AptDdMTTIKEndHhLY0FaQXdYWUJqX0o3QnN5YVFqN3c&output=csv'

# get the resolved entitites
resolvedEntities = {}
f = urllib2.urlopen(resolvedEntitiesUrl)
reader = csv.reader(f)
reader.next() # Skip header line
for row in reader:
    if row[1]:
        resolvedEntities[row[0]]=row[1]
f.close()
# update entIdMap with them
for k,v in resolvedEntities.iteritems():
    entIdMap[k] = set([v])


# and now fix the duped ids
duplicateIdsUrl='https://docs.google.com/spreadsheet/pub?hl=en_US&hl=en_US&key=0AptDdMTTIKEndG5UZ1VzZVJESXhRTFlWTmNPcktCLUE&single=true&gid=0&output=csv'

duplicateIds = {}
f = urllib2.urlopen(duplicateIdsUrl)
reader = csv.reader(f)
reader.next() # Skip header line
for id,rid in reader:
    duplicateIds[id] = rid
# run through entIdMap and fix these
for ent,ids in entIdMap.iteritems():
    dIds = ids.intersection(duplicateIds)
    for id in dIds:
        ids.discard(id)
        ids.update([duplicateIds[id]])
#        print('.')
for id,dupId in duplicateIds.iteritems():
    for k,v in enronIdData.pop(dupId,{}).items():
        enronIdData[id][k].update(v)
    



# now all that's left is to make a database table!

c.execute('drop table if exists people')
c.execute('''
create table people(
    id integer primary key,
    canonId integer unique not null,
    first text,
    middle text,
    last text,
    outsideEmail text
)
''')
# populate:
i = 0
queue=[]
for id,data in enronIdData.iteritems():
    queue.append((
        id,
        unset(data['first']),
        unset(data['middle']),
        unset(data['last']),
        unset(data['outsideEmail'])
    ))
    if len(queue)>=300:
        c.executemany('''
            insert into people(
                canonId,first,middle,last,outsideEmail
            ) values (
                ?,?,?,?,?
            )
        ''',queue)
        queue = []
    i += 1
c.executemany('''
    insert into people(
        canonId,first,middle,last,outsideEmail
    ) values (
        ?,?,?,?,?
    )
''',queue)
# create indices:
c.execute('''
create unique index if not exists
people_canonId on people(canonId)
''')
c.execute('''
create  index if not exists
people_first on people(first)
''')
c.execute('''
create  index if not exists
people_last on people(last)
''')
con.commit()


# create a table mapping entities to canonIds:
c.execute('drop table if exists entityPeople')
c.execute('''
create table entityPeople(
    entity text unique not null,
    canonId text
)
''')
# populate it
queue = []
for ent,ids in entIdMap.iteritems():
    if len(ids)==1: # ignore under- and over-identified entities
        queue.append((ent,unset(ids)))
    if len(queue) >= 300:
        c.executemany('''
            insert into entityPeople(
                entity,canonId
            ) values (
                ?,?
            )
        ''',queue)
        queue=[]
c.executemany('''
    insert into entityPeople(
        entity,canonId
    ) values (
        ?,?
    )
''',queue)
# index it
c.execute('''
create unique index if not exists
entityPeople_entity on entityPeople(entity)
''')
c.execute('''
create  index if not exists
entityPeople_canonId on entityPeople(canonId)
''')


## ### Helper code to find resolve duplicates

## #name the database
dbPath = '../enron.db'
con = sqlite3.connect(dbPath)
con.text_factory = str
c = con.cursor()

c.execute("""  
SELECT Xto, Xcc, Xfrom
FROM rawMessages
""")
messages = c.fetchall()
con.close()


sending_profile = defaultdict(lambda:defaultdict(int))
receiving_profile = defaultdict(lambda:defaultdict(int))
for message in messages :
    (xto, xcc, sender) = message
    try: 
        sender_id = entIdMap[sender]
        if sender_id and len(sender_id) == 1 :
            sender = list(sender_id)[0]
    except KeyError:
        sender = sender

    recipients = parsePluralEntities(xto)
    recipients.extend(parsePluralEntities(xcc))
    
    for recipient in recipients :
        try:
            recipient_id = entIdMap[recipient]
            if recipient_id and len(recipient_id) == 1 :
                recipient = list(recipient_id)[0]
        except :
            recipient = recipient
        sending_profile[sender][recipient] = +1
        receiving_profile[recipient][sender] += 1

with open('entities.csv','w') as f :
    writer = csv.writer(f, dialect="excel")
    writer.writerow(["id", "first", "last"])
    final_enron_ids = set([list(value)[0] for value in entIdMap.values()])
    for enron_id in final_enron_ids :
        try:
            writer.writerow([enron_id,
                             unset(enronIdData[enron_id]['first']),
                             unset(enronIdData[enron_id]['last']),
                             unset(enronIdData[enron_id]['middle']),
                             unset(enronIdData[enron_id]['outsideEmail'])
                             ]
                            )
        except Exception as e:
            print e
            print enron_id
            break 

        


                
            
        

    




