import sqlite3
import datetime
import re
from extractIds import *

#name the database
dbPath = '../enron.db'
con = sqlite3.connect(dbPath)
con.row_factory = sqlite3.Row
con.text_factory = str
c = con.cursor()

c.execute('DROP TABLE IF EXISTS messages')
c.execute('CREATE TABLE messages(id integer primary key, senderId varchar, subject varchar, body varchar, localtime datetime, gmttime datetime)')

c.execute('DROP TABLE IF EXISTS recipients')
c.execute('CREATE TABLE recipients(id integer, recipientId varchar, recipientType varchar, PRIMARY KEY(id, recipientId) ON CONFLICT REPLACE)')


entity_d = {}
c.execute("""
SELECT entity, canonID FROM entityPeople"""
          )

entities = c.fetchall()
for entity in entities :
    entity_d[entity['entity']] = entity['canonId']

          

c.execute("""
SELECT id, Xfrom, Xto, Xcc, Xbcc, Subject, Time, Date, body FROM rawMessages"""
          )


messages = c.fetchall()

for message in messages:
    try:
        senderId = entity_d[message['Xfrom'].lower()]
    except :
        senderId = message['Xfrom']
    (date_string,
     utc_offset_sign,
     utc_offset_hour,
     utc_offset_min,
     tz) = re.split(' *([+-])(\d{2})(\d{2}) *', message['Date'])
    local_time = datetime.datetime.strptime(date_string,
                                            "%a, %d %b %Y %H:%M:%S")
    utc_offset = datetime.timedelta(hours=int(utc_offset_hour),
                                    minutes=int(utc_offset_min)
                                    )
    if utc_offset_sign is '-' :
        gmt_time = local_time - utc_offset
    else :
        gmt_time = local_time - utc_offset

    c.execute("""
    INSERT INTO messages(
      id, senderID, subject, body, localtime, gmttime
      )
      VALUES (
        ?, ?, ?, ?, ?, ?
      )
    """, (message['id'], senderId, message['Subject'], message['body'],
          local_time, gmt_time)
              )
    for recipientType in ['Xto', 'Xcc', 'Xbcc'] : 
        for recipient in parsePluralEntities(message[recipientType]) :
            if recipient is '' :
                continue
            try:
                recipientId = entity_d[recipient.lower()]
            except:
                recipientId = recipient
            c.execute("""
            INSERT INTO recipients(
            id, recipientId, recipientType
            )
            VALUES (
            ?, ?, ?
            )
            """, (message['id'], recipientId, recipientType)
                      )


con.commit()
con.close()
