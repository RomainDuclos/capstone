from cassandra.cluster import Cluster
import time
import statistics

GlobalStart = time.time()

## Pour se connecter au cluster, decommenter la premiere ligne. En local, la deuxieme.
cluster = Cluster(['172.16.134.144', '172.16.134.142', '172.16.134.143'])
# cluster = Cluster()
session = cluster.connect()

# on switch sur le bon KEYSPACE
session.set_keyspace('testcluster')

#variables
nbTotalLignesTheorique = 2500000
nbRange = 10000
nbIter = nbTotalLignesTheorique/nbRange
listTimers = []


for i in range(nbIter):
    if i==0 :
        benchmark = "SELECT sujet, predicat, objet, token(sujet, predicat, objet) from records limit " + str(nbRange) + ";"
    else :
        benchmark = "SELECT sujet, predicat, objet, token(sujet, predicat, objet) from records where token(sujet, predicat, objet) > " + str(token) + " limit " + str(nbRange) + ";"
    start = time.time()
    maRange = session.execute(benchmark)    #on recup tout
    token = maRange[-1][-1]
    end = time.time()
    #liste des temps pour stats
    listTimers.append(end-start)
    #On va chercher le dernier token et on le charge
    # le ifelse est pour eviter que ca plante
    # print(token)


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
print("Temps global d'execution du programme : " + str(GlobalEnd-GlobalStart) + " seconds")
#moyenne=statistics.median(listTimers)
#max = max(listTimers)
#min = min(listTimers)
#print(moyenne)
#print(max-min)
