from cassandra.cluster import Cluster
import time
import statistics

def count(save1):
    i=0
    for lines in save1:
        i=i+1
        # print(i)
    print("save1 contains " + str(i) + " rows")

GlobalStart = time.time()

cluster = Cluster(['172.16.134.144', '172.16.134.142', '172.16.134.143'])
session = cluster.connect()

session.set_keyspace('pkspo')

from cassandra.query import SimpleStatement
query = "SELECT * FROM records limit 1000000"  # users contains 100 rows
statement = SimpleStatement(query, fetch_size=10)
res=session.execute_async(statement)

##timer start

##timer atteint
resultat = res.result()
print(resultat.paging_state)
print(resultat)

cpt=0
ancien = ""
for l in resultat:
    print(l)
    courant = resultat.paging_state
    # print(courant + " ... " + ancien)
    # print(ancien==courant)
    if(cpt>20):
        break;
    cpt = cpt +1
    ancien = courant

statement = SimpleStatement(query, fetch_size=10)
ps = courant
NEWRESULTAT = session.execute(statement, paging_state=ps)
for l in NEWRESULTAT:
    print(l)
    courant = resultat.paging_state
    # print(courant + " ... " + ancien)
    # print(ancien==courant)
    if(cpt>20):
        break;
    cpt = cpt +1
    ancien = courant






# # handler = PagedResultHandler(res)
# print(res)
# print(res.result())
# # count(save0)
# time.sleep(0.0000000000000002)
# print("--------------------------------------------------------------------------------------------")
# print(res)
# save1 = res.result()
# print(save1)
# # count(save1)
# time.sleep(0.0000000000000002)
# print("--------------------------------------------------------------------------------------------")
# print(res)
# save2 = res.result()
# # count(save2)
# print(save2)
#
#

# print("save2 contains " + str(j) + " rows")
