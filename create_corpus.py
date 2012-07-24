import os
import MySQLdb

from gensim import corpora, models, similarities


def convert_recip(val): #take numeric recip and return CRP value; take CRP value and return number for gensim
    if type(val)==int:
        recip = string(val)
        if recip[0]=='1':  recip = 'N' + recip[1:]    
        elif recip[0]=='2':  recip = 'C' + recip[1:]
        return recip
    else:    
        recip = val
        if recip[0]=='N':  recip = '1' + recip[1:]    
        elif recip[0]=='C':  recip = '2' + recip[1:]
        return int(recip)


conn = MySQLdb.connect(db='FEC')
cursor = conn.cursor()

statement = "SELECT * FROM gensim_2_contrib_recip ORDER BY contribidshort"
cursor.execute(statement)
res = cursor.fetchall()

corpus = [] #recipient sets per person
contrib_lookup = [] #index of those people's ids in the same order
recip_lookup = []

curcontrib = None
currecips = []
for r in res:
   if r[0]!=curcontrib:
        if curcontrib: #done with one guy. save him.
            corpus.append(currecips)
            currecips = []
            contrib_lookup.append(curcontrib)
        curcontrib = r[0]
   recip = convert_recip(r[1])
   currecips.append((recip,1)) #no weighing by date, amount, etc. you either gaveor you didn't. 
   if recip not in recip_lookup: recip_lookup.append(recip)


#statement = "DROP TABLE IF EXISTS gensim_similarity"
#cursor.execute(statement)

statement = "CREATE TABLE gensim_similarity (alg varchar(15), source varchar(15), rank int, target varchar(15), similarity float)"
cursor.execute(statement)

tfidf = models.TfidfModel(corpus)
 
model = models.LsiModel(tfidf[corpus], num_topics=100)


#similarity = similarities.SparseMatrixSimilarity(model[tfidf[corpus]],num_best=80)
similarity = similarities.Similarity('corpus',model[tfidf[corpus]],num_best=80)


#pass sim_index an object, and it will return the (in this case) 
#50 most similar objects from the corpus

for i,contrib in enumerate(contrib_lookup):
    print i
    sims = similarity[ model[tfidf[corpus[i]]] ]
    for (j,(target,simval)) in enumerate(sims):
        statement = "INSERT INTO gensim_similarity (alg, source, rank, target, similarity) VALUES ('lsi','%s',%s,'%s',%s)"
        cursor.execute(statement, (contrib_lookup[i], j, contrib_lookup[target], simval))
    conn.commit()



 
 
