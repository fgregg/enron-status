'''
A script to run throught the Enron email as downloaded from
http://www.cs.cmu.edu/~enron/enron_mail_20110402.tgz
and dump the raw emails into an sqlite db.
'''

import os
import sys
import email
import sqlite3
import datetime


#####
# set some paths etc
#####
# this should be the base directory, with a subdirectory for each user
baseMailDir = '../enron_mail_20110402/maildir/'
# name the database
dbPath = '../enron.db'

# list of the headers we want to get
headersList = [
    'Message-ID','Date','From','To','Subject','Mime-Version','Content-Type',
    'Content-Transfer-Encoding','X-From','X-To','X-cc','X-bcc','X-Folder',
    'X-Origin','X-FileName',
    # some strange ones that are in there:
    'Re','Time','Attendees'
]

# convert these to db table names
columnNames = []
for h in headersList:
    if h=='From':
        columnNames.append('From_')
    elif h=='To':
        columnNames.append('To_')
    else:
        columnNames.append(h.replace('-',''))

# make a substitutable string for db inserts
# it has to list the table names and have a '?' for each 
insertString = 'insert into rawMessages(filePath, body, %s) values (?, ?, %s)' % (', '.join(columnNames) , ', '.join(['?']*len(columnNames)))

# set up the database
con = sqlite3.connect(dbPath)
con.text_factory = str # there are a few messages with strange characters.

con.execute('drop table if exists rawMessages')
con.execute('''
    create table rawMessages (
        id integer primary key, filePath varchar, body varchar,
        %s
    )
''' % (' varchar, '.join(columnNames)+' varchar'))

for path,dirs,files in os.walk(baseMailDir):
    if len(files)>0:
        sys.stdout.write('\r\n%s message(s): %s' % (len(files),path))
        sys.stdout.flush()
        messageInsertQueue = []
        for fn in files:
            with open("%s/%s" % (path,fn),'r') as f:
                msg = email.message_from_file(f)
            # first get the filePath
            thisMessage = ['%s/%s' % (path,fn)]
            # append the message body
            thisMessage.append(msg.get_payload())
            # and then the rest of the headers
            for h in headersList:
                # grab the text or empty string if header not present
                thisMessage.append(msg.get(h,''))
            # convert this message to tuple and append to messages.
            messageInsertQueue.append(tuple(thisMessage))
        # put all the messages from this directory into the db
        con.executemany(insertString,messageInsertQueue)
        con.commit()


con.close()                    









# a short (but long-executing) loop to get every header from the emails:
# (for posterity)
if False:
    allHeaders = set()
    for path,dirs,files in os.walk(baseMailDir):
        if len(files)>0:
            sys.stdout.write('\r\n%s message(s): %s' % (len(files),path))
            sys.stdout.flush()
            for fn in files:
                with open("%s/%s" % (path,fn),'r') as f:
                    msg = email.message_from_file(f)
                allHeaders.update(msg.keys())
