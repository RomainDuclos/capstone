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
        if(cpt>3):
            exit()


cluster = Cluster(['172.16.134.144', '172.16.134.142', '172.16.134.143'])
# cluster = Cluster()
session = cluster.connect()

session.set_keyspace('swdf')

#Le paging state permet de recuperer a partir de la prochaine page, mais ca suppose qu'on a eu le temps de lire en entier notre page sinon c'est mmort
# Estt-ce que l'acces au paging state est temps constant, si oui combien on met de temps dans le paging state a revenir ou on etait ?

#Acces temps constant ? =>

#On fait un tour, on stop, et on recommence
query = "SELECT sujet, predicat, objet FROM pos WHERE predicat=$$http://www.w3.org/2002/07/owl#sameAs$$"
tailleFetch = 1
# statement = SimpleStatement(query, fetch_size=2000)
statement = SimpleStatement(query, fetch_size=tailleFetch)
# res = session.execute_async(statement)
# tmp = res.result()
# tmp2 = iter(tmp)
# print(type(tmp2))
# #print(next(tmp2))
# while tmp2.has_more_pages:
#     #print(line)
#     # print(tmp2.paging_state)
#     tmp2.fetch_next_page()
#     print(tmp2.current_rows)
#     # # print(next(tmp2))
#     # print(tmp2.current_rows)
#     # if len(tmp2.current_rows)==0:
#     #     exit()
#     print(tmp2.has_more_pages)
# exit()


etat = ""
# for i in range(0,15000):
cpt = 0
for i in range(0,30000):
    if i==0:
        etat = benchmark(statement, tailleFetch)
    else:
        etat = benchmark(statement, tailleFetch, etat)
        if etat is None:
            # print("break at ")
            # print(i)
            break;
    # if(i%100==0):
    #     print(i)

#Le delete read est bon.
