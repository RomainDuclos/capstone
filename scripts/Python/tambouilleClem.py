from cassandra.cluster import Cluster
import time
import statistics
from cassandra.query import SimpleStatement

def benchmark(statement,tailleFetch, ps=""):   #paging is optional
    # print("--------------------------------------------- ")
    ancien = ""
    resultat = ""
    if ps!= "":
        # res=session.execute_async(statement, paging_state=ps)
        tempsMarqueurDebut = time.time()
        res=session.execute(statement, paging_state=ps)
        tempsMarqueurFin = time.time()
        # print(str(tempsMarqueurFin-tempsMarqueurDebut) + " sec")
        print(str(tempsMarqueurFin-tempsMarqueurDebut))
    else:
        # res=session.execute_async(statement)
        res=session.execute(statement)
    cpt=0
    # resultat = res.result()
    resultat = res
    debut = time.time()
    for l in resultat:
        courant = resultat.paging_state
        # print(l)
        if(cpt>=tailleFetch-1): #Je simule une interruption
            fin = time.time()
            # print(str(fin-debut) + " sec")
            # print(l)
            return courant  #renvoie le paging state ou on a stop
            #Il faut parcourir la page. ca je pense qu'il faut le metttre en atomique
            # Sinon il va pas savoir ou reprendre, lui il ne peut reprendre que a la page d'apres
            # Ceci etant dit ce nest quun parcour de liste donc si la liste ne fait pas 10 pieds de long
            # ca va vite
        cpt=cpt+1


cluster = Cluster(['172.16.134.141', '172.16.134.142', '172.16.134.143'])
#cluster = Cluster()
session = cluster.connect()

session.set_keyspace('pkpos')

#Le paging state permet de recuperer a partir de la prochaine page, mais ca suppose qu'on a eu le temps de lire en entier notre page sinon c'est mmort
# Estt-ce que l'acces au paging state est temps constant, si oui combien on met de temps dans le paging state a revenir ou on etait ?

#Acces temps constant ? =>

#On fait un tour, on stop, et on recommence
query = "SELECT sujet, predicat, objet FROM records WHERE predicat='b'"
tailleFetch = 1000
# statement = SimpleStatement(query, fetch_size=2000)
statement = SimpleStatement(query, fetch_size=tailleFetch)

etat = ""
# for i in range(0,15000):
for i in range(0,3000):
    if i==0:
        etat = benchmark(statement, tailleFetch)
    else:
        etat = benchmark(statement, tailleFetch, etat)
    # if(i%100==0):
    #     print(i)

#Le delete read est bon.
