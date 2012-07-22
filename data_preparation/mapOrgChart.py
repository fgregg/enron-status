'''
A script to read the org-chart data from the google docs and
create and populate the `hierarchy` table in the database.
'''

import sqlite3
import urllib2
import csv
import itertools




# url for google doc org chart in tab-separated format
orgChartUrl = 'https://docs.google.com/spreadsheet/pub?hl=en_US&hl=en_US&key=0AptDdMTTIKEndFd1QzdXcHZvbDRWc2p4YVMwSXpGLXc&output=csv'


#####
# the google docs org chart data
#####
chart_raw = []
f = urllib2.urlopen(orgChartUrl)
reader = csv.reader(f)
reader.next() # Skip header line
for row in reader :
    chart_raw.append(row)
f.close()
chart_cols = list(itertools.izip_longest(*chart_raw))
chart = {
	'id':[int(id) for id in chart_cols[0]],
	'canonId':list(chart_cols[6]),
	'reports_to':list(chart_cols[10])
}
for i,id in enumerate(chart['reports_to']):
	if id:
		chart['reports_to'][i] = int(id)


idCanonIdMap = dict(zip(chart['id'],chart['canonId']))



#####
# Construct the subordination structure
#####

# construct a dictionary to lookup reports_to
reportsTo = {}
reportedTo = {}
for id,rt in zip(chart['id'],chart['reports_to']):
    if rt and rt!=id: #if rt is not empty and is not a loop
        reportsTo[id] = rt
        reportedTo.setdefault(rt,set()).add(id)


# find the root(s) of the org by walking everybody up as far as they can go
# (this could be done way more efficiently (and obviously less elegantly) if necessary)
def findRoot(i):
    try:
        return(findRoot(reportsTo[i])) # recurse!
    except KeyError:
        return(i)
# The following will crash gloriously if there are any (directed) cycles in
# the reportsTo graph. So, um, be sure there aren't?
roots = set(map(findRoot,reportsTo.keys()))

# This function figures out how far 'down the chain'
# each id is from its higher ups. (recursion is fun!)
# It modifies d in place rather than returning anything...
def rIdentifyHierarchy(i,d):
	d[i] = {}
	if i in reportsTo: # if i is not the root
		d[i][reportsTo[i]] = 1
		for j,k in d[reportsTo[i]].items():
			d[i][j]=k+1
	if i in reportedTo: # if i is not a leaf
		for j in reportedTo[i]:
			rIdentifyHierarchy(j,d)
# and this is a wrapper function to make that easier to use
def identifyHierarchy(i):
	assert i not in reportsTo, '%d is not a root' % i
	d = {}
	rIdentifyHierarchy(i,d)
	return(d)

hDists = {}
for r in roots:
	hDists.update(identifyHierarchy(r))



# great! now add everything to the database:
#name the database
dbPath = '../enron.db'
con = sqlite3.connect(dbPath)
con.text_factory = str

c = con.cursor()

# create the hiearchy table
c.execute('drop table if exists hierarchy')
c.execute('''
create table hierarchy(
    subId int,
    supId int,
    distance int
)
''')
#populate it
queue=[]
for i,dists in hDists.iteritems():
	for j,d in dists.iteritems():
		queue.append((idCanonIdMap[i],idCanonIdMap[j],d))
c.executemany('''
    insert into hierarchy(
        subId,supId,distance
    ) values (
        ?,?,?
    )
''',queue)
# and index it
c.execute('''
create  index if not exists
hierarchy_subId on hierarchy(subId)
''')

c.execute('''
create  index if not exists
hierarchy_supId on hierarchy(supId)
''')


con.commit()
