"""
This is an alternate format that just writes the similarity results to a simple CSV instead of generating JSON/HTML
"""


import MySQLdb
import MySQLdb.cursors


import csv
fout = csv.writer(open('test.csv','w'))

conn = MySQLdb.connect(db='FEC', cursorclass=MySQLdb.cursors.DictCursor)
cursor = conn.cursor()

group_size = 12

#get a list of individuals to focus on
statement = "SELECT source, sum(similarity) as weight FROM gensim_similarity WHERE rank<%s GROUP BY source ORDER BY sum(similarity) DESC" % group_size
cursor.execute(statement)

allpeople = [] #keep a list of all people who appear in a box. if you're in one box, exclude all future boxes with you in it to prevent overlap
dogroups = 5
donegroups = []

while len(donegroups)<dogroups:
    top = cursor.fetchone()
    if not top: break
    source = top['source']
    
    cursor2 = conn.cursor()
    statement = "SELECT gensim_3_contrib.*, gensim_2_contrib_recip.*, similarity FROM gensim_similarity INNER JOIN gensim_2_contrib_recip ON (gensim_similarity.target=gensim_2_contrib_recip.contribidshort) INNER JOIN gensim_3_contrib ON (gensim_similarity.target=gensim_3_contrib.contribidshort) WHERE gensim_similarity.source='%s' AND rank<%s  ORDER BY rank" % (source,group_size)
    cursor2.execute(statement)
    
    thisgroup = cursor2.fetchall()
    
    
    #are we gonna skip this box bc of overlap?
    people = set([detail['contribidshort'] for detail in thisgroup])
    if len(people & set(allpeople)):
        print 'skipping overlap'
        continue
    allpeople += people
    
    group = {}
    groupmembers = []
    for detail in thisgroup:
        if detail['contribidshort'] not in group: #we're on to a new guy
            group[ detail['contribidshort'] ] = {}
            person = {'contribid': detail['contribidshort'], 'orgname': detail['orgname'], 'realcode': detail['realcode'] }
            person['name'] = detail['lastname'] + ', ' + detail['first']
            person['state'] = detail['city'] + ', ' + detail['state']
            groupmembers.append( person )
        group[ detail['contribidshort'] ][ detail['recipid'] ] =  detail['SUM(AMOUNT)']


    #get info for cmtes we're drawing
    cands = {}
    statement = "SELECT crp_cmtes.* FROM crp_cmtes INNER JOIN gensim_2_contrib_recip ON (gensim_2_contrib_recip.recipid=crp_cmtes.recipid) WHERE crp_cmtes.cycle=2012 AND gensim_2_contrib_recip.contribidshort='%s'" % source
    cursor2.execute(statement)
    cands_res = cursor2.fetchall()
    for row in cands_res:
        cands[ row['RecipID'] ] =  {'district': row['FECCandID'][:4], 'name': row['PACShort'], 'party': row['Party']} 
    
    
    
    donegroups.append(source)

    #print table
    candlist = cands.keys()
    fout.writerow([])
    fout.writerow(['focus:',source])
    fout.writerow(['','','',]+[cands[x]['name'] for x in candlist])
    for member in groupmembers:
        contribid = member['contribid']
        row = [member['name'], member['orgname'], member['realcode']]
        for cand in candlist:
            if cand in group[contribid]:
                row.append( group[contribid][cand] )
            else:
                row.append( '' )
        fout.writerow(row)

"""
#2: donors who have given to at least 6 people
drop table if exists gensim_2_contrib_recip;
create table gensim_2_contrib_recip select a.* from gensim_1_contrib_recip a, (select contribidshort from gensim_1_contrib_recip group by contribidshort having count(recipid)>5) b where a.contribidshort=b.contribidshort and a.contribidshort<>"" and a.recipid<>""; 
alter table gensim_2_contrib_recip add index(contribidshort);

#3: get info for donors, populating it with sample descriptors that may not be the best
drop table if exists gensim_3_contrib;
create table gensim_3_contrib select contribidshort, max(lastname) as lastname, max(first) as first, max(orgname) as orgname, max(realcode) as realcode, max(city) as city, max(state) as state from crp_indivs WHERE cycle=2012 and contribidshort in (select distinct contribidshort from gensim_2_contrib_recip) group by contribidshort;
alter table gensim_3_contrib add primary key(contribidshort);
"""
