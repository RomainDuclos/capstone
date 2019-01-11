from cassandra.cluster import Cluster
from hdt import HDTDocument
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement
import time
import datetime
from cassandra.util import uuid_from_time, datetime_from_uuid1

# Pour se connecter au cluster, decommenter la premiere ligne. En local, la deuxieme.
cluster = Cluster(
   ['172.16.134.144', '172.16.134.142', '172.16.134.143'],
   load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'))
# cluster = Cluster()


session = cluster.connect()


# Dans ce script on fait une insertion sur une primary key complexe afin de pouvoir faire des requetes avec nos tokens derriere

# Creating keyspace
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS swdf WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    }
    """
)

# on switch sur le bon KEYSPACE
session.set_keyspace('swdf')

##Table with composite PK
# session.execute(
#     """
#     CREATE TABLE IF NOT EXISTS records (
#     PK timeuuid,
#     sujet text,
# 	predicat text,
# 	objet text,
# 	PRIMARY KEY ( (PK, sujet) , predicat, objet)
#     );
#     """
# )

#Table with column family
session.execute(
    """
    CREATE TABLE IF NOT EXISTS spo (
    sujet text,
	predicat text,
	objet text,
	PRIMARY KEY (sujet,predicat, objet)
    );
    """
)

session.execute(
    """
    CREATE TABLE IF NOT EXISTS osp (
    sujet text,
	predicat text,
	objet text,
	PRIMARY KEY (objet, sujet, predicat)
    );
    """
)

session.execute(
    """
    CREATE TABLE IF NOT EXISTS pos (
    sujet text,
	predicat text,
	objet text,
	PRIMARY KEY (predicat, objet, sujet)
    );
    """
)

i=0
document = HDTDocument("../Data/swdf-2017.hdt")


# Fetch all triples that matches { ?s ?p ?o }
# Use empty strings ("") to indicates variables
(data, cardinality) = document.search_triples("", "", "")
# data = open("../Data/shortdata.nt")

insert = "BEGIN BATCH "

GlobalStart = time.time()
batch = BatchStatement()

for line in data:
    i = i+1
    triple = line
    # triple = line.split(' ')
    # triple[2] = triple[2].rstrip()
    ## L'insertion qui suit est si on a un UUID
    # test = "INSERT INTO records (pk, sujet, predicat, objet) VALUES (" + str(uuid_from_time(datetime.datetime.now())) +" , $$" + triple[0].replace("$", "\$") + "$$, $$" + triple[1].replace("$", "\$") + \
    # "$$, $$" + triple[2].replace("$", "\$") + "$$ );"
    #Inssertion classique
    test = "INSERT INTO spo (sujet, predicat, objet) VALUES ($$" + triple[0].replace("$", "\$") + "$$, $$" + triple[1].replace("$", "\$") + \
    "$$, $$" + triple[2].replace("$", "\$") + "$$ );"
    batch.add(SimpleStatement(test))
    if(i%50==0):
        session.execute(batch)
        batch = BatchStatement()
        print("row inserted : " + str(i))
    # print(test)

    test = "INSERT INTO osp (sujet, predicat, objet) VALUES ($$" + triple[0].replace("$", "\$") + "$$, $$" + triple[1].replace("$", "\$") + \
    "$$, $$" + triple[2].replace("$", "\$") + "$$ );"
    batch.add(SimpleStatement(test))
    if(i%50==0):
        session.execute(batch)
        batch = BatchStatement()
        print("row inserted : " + str(i))

    test = "INSERT INTO pos (sujet, predicat, objet) VALUES ($$" + triple[0].replace("$", "\$") + "$$, $$" + triple[1].replace("$", "\$") + \
    "$$, $$" + triple[2].replace("$", "\$") + "$$ );"
    batch.add(SimpleStatement(test))
    if(i%50==0):
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
