from cassandra.cluster import Cluster
import time

cluster = Cluster(['172.16.134.144', '172.16.134.142', '172.16.134.143'])
session = cluster.connect()

session.set_keyspace('pkspo')

def queryComplexity(query, name):
    start = time.time()*1000
    resp = session.execute(query)
    end = time.time()*1000
    nb = 0
    for r in resp:
        nb += 1
    print(name+","+str(end-start)+","+str(nb))

print("#id, time, nbResult")

# SPO
query = "SELECT * FROM records WHERE sujet='a' and predicat='b' and objet='c1' ALLOW FILTERING"
queryComplexity( query, "SPO")
# S..
# query = "SELECT * FROM records WHERE sujet='a' ALLOW FILTERING;"
# queryComplexity( query, "S..")
# # SP.
# query = "SELECT * FROM records WHERE sujet='a' and predicat='b' ALLOW FILTERING;"
# queryComplexity( query, "SP.")
# S.O
query = "SELECT * FROM records WHERE sujet='a' and objet='c' ALLOW FILTERING;"
queryComplexity( query, "S.O")
# #.P.
# query = "SELECT * FROM records WHERE predicat='b' ALLOW FILTERING;"
# queryComplexity( query, ".P.")
# # #.PO
# query = "SELECT * FROM records WHERE predicat='b' and objet='c' ALLOW FILTERING;"
# queryComplexity( query, ".PO")
# ..O
query = "SELECT * FROM records WHERE objet='c' ALLOW FILTERING;"
queryComplexity( query, "..O")
# # ...
# query = "SELECT * FROM records;"
# queryComplexity( query, "...")


# query = "SELECT count(*) FROM records ALLOW FILTERING;"
# queryComplexity( query, "COUNT")
