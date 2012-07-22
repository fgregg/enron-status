'''
Some functions for extracting identifying information from
the enron email fields. The function extractId() should be able to take
any individual entity from fields like To, XTo, From, etc and get
all the useful id info from it
'''

import re

# compile some REs here
bracketsRE = re.compile(r'[\' \"]*([^\"]*?)[\'\"\s]*<[\'\"\s]*([^\'\"]*)[\'\"\s]*>')
emailRE = re.compile(r'(\?\?S)?([A-Z0-9._%+-]+)@([A-Z0-9.-]+\.[A-Z]{2,4})',re.IGNORECASE)
# (there's a weird case where an email like "abc@enron.com" will be listed "??Sabc@enron.com")
dotRE = re.compile(r'(\S*)\s*[\.]+\s*(\S*)')
commaRE = re.compile(r'\s*([\w\- \']*)\s*\,+\s*([\w\- \']*)\s*')
spaceRE = re.compile(r'\s*([\w\- \'\.]+)\s+([\w\- \'\.]+)\s*')
enronTagRE = re.compile(r'CN=([^=>\+\-]+)$',re.IGNORECASE)


def nameOrEmail(s):
    '''
    A helper function for extractId. s is a string like "First Last" or
    "First Middle Last" or "first.last@enron.com" or "Last, First", etc.
    This function tries to figure out which and give us the parsed result as a dict
    '''
    # oh yea: if-else bonanza (there must be a cleaner way to do this...)

    eml = emailRE.search(s)
    if eml:
        # looks like an email address, so parse it
        if eml.group(3)[-9:].lower()!='enron.com':
            # all we have is a generic email
            return({'outsideEmail':eml.group(0)})
        else:
            dots1 = dotRE.search(eml.group(2))
            if dots1:
                dots2 = dotRE.search(dots1.group(1))
                if dots2:
                    # there are two dots in the email
                    return({'first':dots2.group(1),'middle':dots2.group(2),'last':dots1.group(2)})
                else:
                    # there is one dot in the email
                    return({'first':dots1.group(1),'last':dots1.group(2)})
            elif ('-' not in eml.group(2) and '+' not in eml.group(2)):
                # there are no dots in the email
                return({'emailId':eml.group(2).strip()})
            else:
                return({})
 
    else:
        # not an email
        cma = commaRE.search(s)
        if cma:
            # it has a comma, so Last, First or Last, First Middle
            spc = spaceRE.search(cma.group(2))
            if spc:
                # Last, First Middle
                return({'first':spc.group(1),'middle':spc.group(2),'last':cma.group(1)})
            else:
                # Last, First
                return({'first':cma.group(2),'last':cma.group(1)})
        else:
            # no comma, so either "First", "First Last" or "First Middle Last"
            spc1 = spaceRE.search(s)
            if spc1:
                # at least has Last
                spc2 = spaceRE.search(spc1.group(1))
                if spc2:
                    # all three names
                    return({'first':spc2.group(1),'middle':spc2.group(2),'last':spc1.group(2)})
                else:
                    # First Last
                    return({'first':spc1.group(1),'last':spc1.group(2)})
            else:
                # just one name, which we assume is first
                return({'first':s.strip()})



# a first attempt at extracting whatever information exists in the 'entities'
def extractId(e):
    '''
    given a string `e` that came from any of the *from or *to fields,
    return a tuple with (possibly empty) values:
        (first, middle, last, emailId, outsideEmail)
    '''
    # rather than try to fit it all into regular expressions, will
    # break it into sub-problems:
    # if it has `<...>`:
    #   see if it has an Enron emailId
    #   otherwise grab the 'outside' email address
    # else if it looks like just an email address:
    #   grab the address
    # else:
    #   it's just a name, so get f,m,l from there.


    res = dict.fromkeys(['first','middle','last','emailId','outsideEmail'],None)
    for k in res.keys():
        res[k] = set()

    br = bracketsRE.search(e)
    if br:
        # we have brackets, so we have two pieces of information
        name,tag = br.groups()
        # start with `tag` (the text in the bracket)
        # first: is it the internal (outlook?) info?
        enronTag = enronTagRE.search(tag)
        if enronTag:
            enron_tag = enronTag.groups(1)[0]
            enron_tag = enron_tag.lower().strip().strip('.')
            enron_tag = re.sub(r'^mbx\_',
                               '',
                               enron_tag
                               )
            res['emailId'].add(enron_tag.strip())
        else:
            # no outlook-style tag, so see what other info is in there
            for k,v in nameOrEmail(tag).iteritems():
                res[k].add(v.lower().strip().strip('.'))
        # and now do the same for the part before the brackets
        for k,v in nameOrEmail(name).iteritems():
            res[k].add(v.lower().strip().strip('.'))
    else:
        # no brackets
        for k,v in nameOrEmail(e).iteritems():
            res[k].add(v.lower().strip().strip('.'))

    # get rid of any empty strings that ended up in there
    for v in res.values():
        try:
            v.remove('')
        except KeyError:
            pass

    return(res)

def parsePluralEntities(string) :
    plurals = [
        ## Kevin Presto, Phillip Platter
        r"[^ ,]+ +[^,]+,",
        ## <foo@foobar.com>,  <bar@foobar.com>
        r">,",         
        ## @uc.edu',; cu.com.br, @Enron, @ETC;
        r"@[^,]*,",
        ]
    plurals_re = "(" + "|".join(plurals) + ")"

    entities = []

    entity_search = re.finditer(plurals_re, string)
    start = 0
    for entity in entity_search :
        entities.append(string[start:(entity.end()-1)].strip())
        start = entity.end()
    entities.append(string[start:].strip())

    return entities



if __name__ == "__main__":
    import numpy as np
    i = np.random.randint(len(entities));e=list(entities)[i];print(e+'\n');print(extractId(e))

# dysfunctional corners:
#
# 'Jeff_Dasovich@ees.enron.com' should be enronId=Jeff_Dasovich or
# name=Jeff Dasovich?
#
# 'drew_fossum@enron.com' same question. These are determened by
# inclusion of '_' in dotRE
#
# 'Robert C Williams AT ENRON_DEVELOPMENT@CCMAIL @ ENRON' No clue how
# to get what we want from this
#
# 'Stan Horton- Chairman and CEO@ENRON'
#
# *unless* we restrict names to at most 3 words? but then what if this
# had no middle initial?
#
# 'wizzard69@usa.net@ENRON
# <IMCEANOTES-wizzard69+40usa+2Enet+40ENRON@ENRON.com>' thinks that
# 'IMCEANOTES-wizzard69+40usa+2Enet+40ENRON' is an enronId
#
# 'edetective@gmx.at@ENRON
# <IMCEANOTES-edetective+40gmx+2Eat+40ENRON@ENRON.com>' same
#
# 'Brian.Hendon@ENRONCOMMUNICATIONS.nt.ect.enron.com.enron.net' uhhhhh....
#
# 'Schulmeyer Gerhard <Gerhard.Schulmeyer@sc.siemens.com>@ENRON' gets
# name order wrong?
