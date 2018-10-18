from cassandra.cluster import Cluster
import time
import statistics

cluster = Cluster()
session = cluster.connect()
# on switch sur le bon KEYSPACE
session.set_keyspace('jekasstout')

#on recupere l ensemble des triples
triples = session.execute("SELECT sujet, predicat, objet, token(sujet, predicat, objet) from records;")
i=0
listToken = []

#on liste les triples precedents et on garde un token tout les 1000 tokens
for triple in triples:
    i = i+1
    if(i%10000==0):
        token = triple[3]
        listToken.append(token)
#        print(i)
#        print(token)cd
#    print(triple[4])    #triple[0] = S , [1] = P, etc. [3] = token
print("Nombre de lignes lues : " + str(i))

#calcul des temps d exec pour recuperer une ligne associe a un token de la liste
listTimers = []
for token in listToken:
    #query
    benchmark = "SELECT sujet, predicat, objet, token(sujet, predicat, objet) from records where token(sujet, predicat, objet) = " + str(token) + ";"
    #exec
    start = time.time()
    session.execute(benchmark)
    end = time.time()
    #liste des temps pour stats
    listTimers.append(end-start)

# stats
i=0
for timer in listTimers:
    i = i+1
    print(timer)
print("total de " + str(i) + " tokens lus")
#moyenne=statistics.median(listTimers)
#max = max(listTimers)
#min = min(listTimers)
#print(moyenne)
#print(max-min)
