from cassandra.cluster import Cluster
import time
import statistics

GlobalStart = time.time()

cluster = Cluster()
session = cluster.connect()
# on switch sur le bon KEYSPACE
session.set_keyspace('jekasstout')

#variables
nbTotalLignesTheorique = 2900000
nbRange = 2000

#on recupere l ensemble des triples
start = time.time()
triples = session.execute("SELECT sujet, predicat, objet, token(sujet, predicat, objet) from records limit " + str(nbTotalLignesTheorique) + ";")
end = time.time()
i=0
listToken = []

#on liste les triples precedents et on garde un token tout les 1000 tokens
for triple in triples:
    i = i+1
    if(i%nbRange==0):
        token = triple[3]
        listToken.append(token)
#        print(i)
#        print(token)cd
#    print(triple[4])    #triple[0] = S , [1] = P, etc. [3] = token

bilanTotalLignes = "Nombre de lignes totales lues/Nombre de ligne demandees : " + str(i) + "/" + str(nbTotalLignesTheorique) + " en " + str(end-start) + "sec."


#calcul des temps d exec pour recuperer une ligne associe a un token de la liste
listTimers = []
for token in listToken:
    #query
    benchmark = "SELECT sujet, predicat, objet, token(sujet, predicat, objet) from records where token(sujet, predicat, objet) > " + str(token) + " limit " + str(nbRange) + ";"
    #execca
    start = time.time()
    session.execute(benchmark)
    end = time.time()
    #liste des temps pour stats
    listTimers.append(end-start)

# stats
i=0
for timer in listTimers:
    i = i+1
    monTimer = str(timer)
    while(len(monTimer)<15):
        monTimer += "0"
    print(monTimer + " seconds")

GlobalEnd = time.time()

print("total de " + str(i) + " tokens lus sur des ranges de " + str(nbRange) + " lignes")
print(bilanTotalLignes)
print("Temps global d'execution du programme : " + str(GlobalEnd-GlobalStart) + " seconds")
#moyenne=statistics.median(listTimers)
#max = max(listTimers)
#min = min(listTimers)
#print(moyenne)
#print(max-min)
