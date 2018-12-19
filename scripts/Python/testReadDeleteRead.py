from cassandra.cluster import Cluster
import time
import statistics
from cassandra.query import SimpleStatement


# cluster = Cluster(['172.16.134.144', '172.16.134.142', '172.16.134.143'])
cluster = Cluster()
session = cluster.connect()

session.set_keyspace('pktest')

#Le paging state permet de recuperer a partir de la prochaine page, mais ca suppose qu'on a eu le temps de lire en entier notre page sinon c'est mmort
# Estt-ce que l'acces au paging state est temps constant, si oui combien on met de temps dans le paging state a revenir ou on etait ?

#Acces temps constant ? =>

#On fait un tour, on stop, et on recommence
query = "SELECT * FROM records"
tailleFetch = 7
# statement = SimpleStatement(query, fetch_size=2000)
statement = SimpleStatement(query, fetch_size=tailleFetch)
res = session.execute(statement)

cpt=0
courant = ""
for l in res:
    print(l)
    courant = res.paging_state
    # print(courant)
    if(cpt>=6):
        break;
    cpt = cpt+1

print("---------------------------------------------------------")

session.execute("DELETE FROM records WHERE sujet='a' and predicat='b0'")
session.execute("DELETE FROM records WHERE sujet='a' and predicat='b1'")
session.execute("DELETE FROM records WHERE sujet='a' and predicat='b2'")
session.execute("DELETE FROM records WHERE sujet='a' and predicat='b3'")
session.execute("DELETE FROM records WHERE sujet='a' and predicat='b4'")
session.execute("DELETE FROM records WHERE sujet='a' and predicat='b5'")


cpt=0
res2 = session.execute(statement, paging_state=courant)
for l2 in res2:
    print(l2)
    courant = res2.paging_state
    # print(courant)
    if(cpt>=6):
        break;
    cpt = cpt+1
