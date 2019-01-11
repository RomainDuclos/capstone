from cassandra.cluster import Cluster
import time
import statistics
from cassandra.query import SimpleStatement


def parcourir(resultat):
    cpt =0
    tmp2 = iter(res)
    while tmp2.has_more_pages:
        print(tmp2.current_rows)


cluster = Cluster(['172.16.134.144', '172.16.134.142', '172.16.134.143'])
# cluster = Cluster()
session = cluster.connect()
session.set_keyspace('test')


#Le paging state permet de recuperer a partir de la prochaine page, mais ca suppose qu'on a eu le temps de lire en entier notre page sinon c'est mmort
# Estt-ce que l'acces au paging state est temps constant, si oui combien on met de temps dans le paging state a revenir ou on etait ?

#Acces temps constant ? =>

#On fait un tour, on stop, et on recommence
query = "SELECT * FROM test"
tailleFetch = 1
# statement = SimpleStatement(query, fetch_size=2000)
statement = SimpleStatement(query, fetch_size=tailleFetch)
res = session.execute(statement)

ps = ""
for l in res:
    # print(l)
    ps = res.paging_state
    # print(ps)

# print("-----------------------------")
# print("on relance")
#on relance lexec
statement = SimpleStatement(query, fetch_size=tailleFetch)
res = session.execute(statement)

#on sarrete a 5 valeurs
ps = ""
cpt =0
tmp2 = iter(res)
print(tmp2.current_rows)
while tmp2.has_more_pages:
    if(cpt==5):
        # print("-----------------------------")
        print("")
        print("We stopped here : ")
        print(tmp2.current_rows)
        # print("Dans cet etat : ")
        # print(ps)
        print("")
        break
    ps = res.paging_state
    tmp2.fetch_next_page()
    print(tmp2.current_rows)
    # print(ps)
    cpt = cpt +1

print("")
print("We delete the following lines including state where we stopped and need to resume")
session.execute("DELETE FROM test where sujet='a17'")
session.execute("DELETE FROM test where sujet='a5'")
session.execute("DELETE FROM test where sujet='a13'")
print("DELETE FROM test where sujet='a17'")
print("DELETE FROM test where sujet='a5'")
print("DELETE FROM test where sujet='a13'")
print("We resume from a deleted state")
print("")

#On relance une execution a partir de l'etat ou on s'est arrete
statement = SimpleStatement(query, fetch_size=tailleFetch)
res = session.execute(statement, paging_state=ps)

tmp2 = iter(res)
print(tmp2.current_rows)
while tmp2.has_more_pages:
    tmp2.fetch_next_page()
    print(tmp2.current_rows)


exit()

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

# session.execute("DELETE FROM records WHERE sujet='a' and predicat='b0'")
# session.execute("DELETE FROM records WHERE sujet='a' and predicat='b1'")
# session.execute("DELETE FROM records WHERE sujet='a' and predicat='b2'")
# session.execute("DELETE FROM records WHERE sujet='a' and predicat='b3'")
# session.execute("DELETE FROM records WHERE sujet='a' and predicat='b4'")
# session.execute("DELETE FROM records WHERE sujet='a' and predicat='b5'")


cpt=0
res2 = session.execute(statement, paging_state=courant)
for l2 in res2:
    print(l2)
    courant = res2.paging_state
    # print(courant)
    if(cpt>=6):
        break;
    cpt = cpt+1
