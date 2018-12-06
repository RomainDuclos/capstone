from cassandra.cluster import Cluster
import time

cluster = Cluster(['172.16.134.144', '172.16.134.142', '172.16.134.143'])
session = cluster.connect()



def queryComplexity(query, name):
    start = time.time()*1000
    resp = session.execute(query)
    end = time.time()*1000
    print("\n"+name+"\n")
    print("query: \t")
    print("time: \t"+str(end-start)+" ms")
    return resp
    # nb = 0
    # for r in resp:
    #     nb += 1
    # print("nb: "+str(nb)+"\n")



# # SPO
# query = "SELECT * FROM records WHERE sujet='a' and predicat='b' and objet='c' ALLOW FILTERING"
# queryComplexity( query, "SPO")

session.set_keyspace('pks')
# session.set_keyspace('ourdataset')
# S..
query = "SELECT * FROM records WHERE sujet='a' ALLOW FILTERING;"
listPKS = session.execute(query)

session.set_keyspace('pkspo')
query = "SELECT * FROM records WHERE sujet='a' ALLOW FILTERING;"
listPKSPO = session.execute(query)


# Parcours manuel parce que flemme

# # SP.
# query = "SELECT * FROM records WHERE sujet='a' and predicat='b' ALLOW FILTERING;"
# queryComplexity( query, "SP.")
# # S.O
# query = "SELECT * FROM records WHERE sujet='a' and objet='c' ALLOW FILTERING;"
# queryComplexity( query, "S.O")


# # .P.
# query = "SELECT * FROM records WHERE predicat='b' ALLOW FILTERING;"
# queryComplexity( query, ".P.")
# # .PO
# query = "SELECT * FROM records WHERE predicat='b' and objet='c' ALLOW FILTERING;"
# queryComplexity( query, ".PO")
# # ..O
# query = "SELECT * FROM records WHERE objet='c' ALLOW FILTERING;"
# queryComplexity( query, "..O")
# # ...
# query = "SELECT * FROM records;"
# queryComplexity( query, "...")
