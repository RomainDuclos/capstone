from cassandra.cluster import Cluster
from hdt import HDTDocument
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement
import time

cluster = Cluster(
    ['172.16.134.144', '172.16.134.142', '172.16.134.143'],
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'))
# cluster = Cluster()


session = cluster.connect()


# Dans ce script on fait une insertion sur une primary key complexe afin de pouvoir faire des requetes avec nos tokens derriere

# Creating keyspace
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    }
    """
)

# on switch sur le bon KEYSPACE
session.set_keyspace('test')

session.execute(
    """
    CREATE TABLE IF NOT EXISTS records (
    sujet text,
	predicat text,
	objet text,
	PRIMARY KEY ((sujet, predicat, objet))
    );
    """
)

#session.execute(
#	"""
#	CREATE INDEX IF NOT EXISTS indexSujet ON testhdt.records ( KEYS (sujet) );
#	"""
#)
#session.execute(
#	"""
#	CREATE INDEX IF NOT EXISTS indexPredicat ON testhdt.records ( KEYS (predicat) );
#	"""
#)
#session.execute(
#	"""
#	CREATE INDEX IF NOT EXISTS indexObjet ON testhdt.records ( KEYS (objet) );
#	"""
#)

i=0
data = open("testdata.nt")

insert = "BEGIN BATCH "

GlobalStart = time.time()
batch = BatchStatement()

for line in data:
    i = i+1
    triple = line.split(' ')
    triple[2] = triple[2].rstrip()
    test = "INSERT INTO records (sujet, predicat, objet) VALUES ($$" + triple[0].replace("$", "\$") + "$$, $$" + triple[1].replace("$", "\$") + \
    "$$, $$" + triple[2].replace("$", "\$") + "$$ );"
    batch.add(SimpleStatement(test))
    if(i%10000==0):
        session.execute(batch)
        batch = BatchStatement()
        print("row inserted : " + str(i))

#fini de vider les requests
if(batch):
    session.execute(batch)
    batch = BatchStatement()
    print("row inserted : " + str(i))


GlobalEnd = time.time()
print("temps total " + str(GlobalEnd-GlobalStart) + " " + str(i) +" lignes inserees")

# i=0
# # print("cardinality of { ?s ?p ?o }: %i" % 10)
# for triple in data:
#     i = i+1
#     test = "INSERT INTO records (sujet, predicat, objet) VALUES ($$" + triple[1][0].replace("$", "\$") + "$$, $$" + triple[1][1].replace("$", "\$") + \
#     "$$, $$" + triple[1][2].replace("$", "\$") + "$$ );"
#     print(test)
# #    if i>1:
# #        exit(1)
#     session.execute(test)
#     print(i)

# print("done")
# #En fait ici j'ai un truc INDEX qui se rajoute, donc je tape ssur triple[1] pour aller chercher ma data
# # triple[0] = mon index
