from cassandra.cluster import Cluster
import time
import statistics

cluster = Cluster()
session = cluster.connect()
# on switch sur le bon KEYSPACE
session.set_keyspace('jekasstout')

triples = session.execute("SELECT sujet, predicat, objet, token(sujet, predicat, objet) from records;")
i=0
listToken = []

for triple in triples:
    i = i+1
    if(i%1000==0):
        token = triple[3]
        listToken.append(token)
#        print(i)
#        print(token)
#    print(triple[4])    #triple[0] = S , [1] = P, etc. [3] = token


listTimers = []
for token in listToken:
    benchmark = "SELECT sujet, predicat, objet, token(sujet, predicat, objet) from records where token(sujet, predicat, objet) = " + str(token) + ";"
    start = time.time()
    session.execute(benchmark)
    end = time.time()
    listTimers.append(end-start)

# stats

# moyenne=statistics.median(listTimers)
# max = max(listTimers)
# min = min(listTimers)
# print(moyenne)
# print(max-min)
