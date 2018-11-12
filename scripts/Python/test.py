from cassandra.cluster import Cluster
import time

cluster = Cluster(['172.16.134.144', '172.16.134.142', '172.16.134.143'])
session = cluster.connect()

session.set_keyspace('testcluster')

benchmark = "SELECT sujet, objet FROM records WHERE predicat='http://www.w3.org/2002/07/owl#sameAs' ALLOW FILTERING;"
#benchmark = "SELECT sujet, predicat, objet FROM records WHERE sujet='http://wordnet-rdf.princeton.edu/wn31/image-n#1-n' ALLOW FILTERING;";
start = time.time()
maRange = session.execute(benchmark)
end = time.time()
print("query time = "+str(end-start)+"s")

start = time.time()
lastTriple = maRange[-1]
end = time.time()
print("access time = "+str(end-start)+"s")

i=0

# start = time.time()
for r in maRange:
    i += 1
    # end = time.time()
    # if (end-start) > 0.70:
    #     break;
    # print(r)

print (i)
