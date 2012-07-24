"""
Query the similarity values we stored in a database, and store resulting clusters in JSON files uploaded to S3. This can sometimes take a little time,
so we don't do it on the fly online.

You'll need to also put clusters.html online.
"""

import simplejson
import math

import MySQLdb
import MySQLdb.cursors

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from clusterssettings.py import AWS_KEY, AWS_SECRET, AWS_BUCKET


c = S3Connection(AWS_KEY,AWS_SECRET)
bucket = c.get_bucket(AWS_BUCKET)



conn = MySQLdb.connect(db='FEC', cursorclass=MySQLdb.cursors.DictCursor)
cursor = conn.cursor()


perfile = 15 #print squares to json 15 at a time

def go():
    algorithm = 'tfidf'
    for height in range(23,36):
        print 'generating %s' % height
        makesquares(height,130,algorithm)

def makesquares(height,dogroups,algorithm):

    #get a list of individuals to focus on
    statement = "SELECT source, sum(similarity) as weight FROM gensim_similarity WHERE rank<%s GROUP BY source ORDER BY sum(similarity) DESC" % height
    cursor.execute(statement)

    allpeople = [] #keep a list of all people who appear in a box. if you're in one box, exclude all future boxes with you in it to prevent overlap
    donegroups = []
    pagejson = []

    while len(donegroups)<dogroups:
        top = cursor.fetchone()
        if not top: break
        source = top['source']
        
        cursor2 = conn.cursor()
        statement = "SELECT gensim_3_contrib.*, gensim_2_contrib_recip.*, similarity FROM gensim_similarity INNER JOIN gensim_2_contrib_recip ON (gensim_similarity.target=gensim_2_contrib_recip.contribidshort) INNER JOIN gensim_3_contrib ON (gensim_similarity.target=gensim_3_contrib.contribidshort) WHERE gensim_similarity.source='%s' AND gensim_similarity.alg='%s' AND rank<%s  ORDER BY rank" % (source,algorithm,height)
        cursor2.execute(statement)
        
        thisgroup = cursor2.fetchall()
        
        
        #are we gonna skip this box bc of overlap?
        people = set([detail['contribidshort'] for detail in thisgroup])
        if len(people & set(allpeople))>1:
            #print 'skipping overlap'
            continue
        allpeople += people
        
        group = {}
        groupmembers = []
        for detail in thisgroup:
            if detail['contribidshort'] not in group: #we're on to a new guy
                group[ detail['contribidshort'] ] = {}
                person = {'contribid': detail['contribidshort'], 'orgname': detail['orgname'], 'realcode': detail['realcode'], 'state': detail['state'] }
                person['name'] = detail['lastname'] + ', ' + detail['first']
                person['city'] =  detail['city']
                groupmembers.append( person )
            group[ detail['contribidshort'] ][ detail['recipid'] ] =  detail['SUM(AMOUNT)']


        #get info for cmtes we're drawing
        cands = {}
        statement = "SELECT crp_cmtes.* FROM crp_cmtes INNER JOIN gensim_2_contrib_recip ON (gensim_2_contrib_recip.recipid=crp_cmtes.recipid) WHERE crp_cmtes.cycle=2012 AND gensim_2_contrib_recip.contribidshort='%s'" % source
        cursor2.execute(statement)
        cands_res = cursor2.fetchall()
        for row in cands_res:
            district = row['FECCandID'][2:4]
            if district in ('RE','00'): district=''
            cands[ row['RecipID'] ] =  {'district': district, 'name': row['PACShort'], 'party': row['Party'], 'congcmtes': [] } 
        
        candlist = cands.keys()
        candinclause = "'" + "','".join(candlist) + "'"
        
        #a) find frequent cmtes
        statement = """select name, committeeid, count(*) n from people INNER JOIN people_roles ON (people.id=people_roles.personid) INNER JOIN people_committees ON (people.id=people_committees.personid) WHERE startdate>="2011-01-01" AND osid IN (%s) GROUP BY name, committeeid HAVING count(*)>2 ORDER BY count(*) DESC""" % candinclause
        cursor2.execute(statement)
        congcmtes_res = cursor2.fetchall()

        congcmtes = [{'name': x['name'].replace('Committee on ',''), 'committeeid': x['committeeid']} for x in congcmtes_res]
        cmtelist = [x['committeeid'] for x in congcmtes_res]
        cmteinclause = "'" + "','".join(cmtelist) + "'"
        
        #b) find who's on those cmtes
        statement = """SELECT people.osid, people_committees.committeeid FROM people INNER JOIN people_roles ON (people.id=people_roles.personid) INNER JOIN people_committees ON (people.id=people_committees.personid) WHERE osid IN (%s) AND committeeid IN (%s) AND startdate>="2011-01-01"
        ORDER BY name desc;""" % (candinclause, cmteinclause)
        cursor2.execute(statement)
        congcmtes_res = cursor2.fetchall()
        
        for row in congcmtes_res:
            cands[ row['osid'] ]['congcmtes'].append( row['committeeid'] )

        #c) attempt to order by cmte
        orderedcands = []
        for cmte in cmtelist:
            for cand in candlist: 
                if cand not in orderedcands: 
                    if cmte in cands[cand]['congcmtes']:
                        orderedcands.append(cand)
   
        #don't forget recipients that aren't on any cmtes
        for cand in candlist: 
            if cand not in orderedcands: 
                orderedcands.append(cand)
                     
        
        
        #print table
        squarejson = { 'candorder': orderedcands, 'cands': cands, 'members': [], 'congcmtes': congcmtes }
        for member in groupmembers:
            contribid = member['contribid']
            otherdonations = len(group[contribid]) - len(set(candlist) & set(group[contribid].keys())) 
            memberjson = {'contribid': contribid, 'name': member['name'], 'state': member['state'], 'org': member['orgname'], 'code': member['realcode'], 'contribs': [], 'otherdonations': otherdonations } 
            for cand in candlist:
                if cand in group[contribid]:
                    memberjson['contribs'].append( group[contribid][cand] )
                else:
                    memberjson['contribs'].append( 0 )
            squarejson['members'].append( memberjson )

        pagejson.append(squarejson)
        
        donegroups.append(source)



        if len(pagejson)==perfile: 
            out = 'parseResults(' + simplejson.dumps(pagejson, use_decimal=True) + ')'
            pagenum = int(math.floor(len(donegroups)/float(perfile)))
            #fout = open('squares/h%s.%s.json' % (height, pagenum),'w')
            #fout.write( 'parseResults(' + simplejson.dumps(pagejson, use_decimal=True) + ')' )
            #fout.close()
            k = Key(bucket)
            k.key = 'squares/tdif.h%s.%s.json' % (height, pagenum)
            print k.key
            k.set_contents_from_string(out, headers={ 'Content-Type': 'application/jsonp' }, policy='public-read')
            k.make_public()
            pagejson = []



go()




