from cassandra.cluster import Cluster
import time
import statistics

GlobalStart = time.time()

cluster = Cluster()
session = cluster.connect()
# on switch sur le bon KEYSPACE
session.set_keyspace('jekasstout')

#variables
nbTotalLignesTheorique = 3000000
nbRange = 2000
nbIter = nbTotalLignesTheorique/nbRange
listTimers = []


for i in range(nbIter):
    if i==0 :
        benchmark = "SELECT sujet, predicat, objet, token(sujet, predicat, objet) from records limit " + str(nbRange) + ";"
    else :
        benchmark = "SELECT sujet, predicat, objet, token(sujet, predicat, objet) from records where token(sujet, predicat, objet) > " + str(token) + " limit " + str(nbRange) + ";"
    start = time.time()
    maRange = session.execute(benchmark)    #on recup tout
    end = time.time()
    #liste des temps pour stats
    listTimers.append(end-start)
    #On va chercher le dernier token et on le charge
    # le ifelse est pour eviter que ca plante
    if maRange:
        token = maRange[-1][-1]
    else:
        break
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
