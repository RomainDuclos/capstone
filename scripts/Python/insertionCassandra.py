from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement
import sys
import os
import time
from progressbar import progressbar
#
cluster = Cluster()
#
#
session = cluster.connect()
# Create keyspace
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    }
    """
)
# Use keyspace
session.set_keyspace('test')
# Create records table
session.execute(
    """
    CREATE TABLE IF NOT EXISTS records (
    sujet text,
	predicat text,
	objet text,
	PRIMARY KEY (sujet, predicat, objet)
    );
    """
)
#session.execute("CREATE INDEX index_s ON test.records (sujet);")
session.execute("CREATE CUSTOM INDEX index_p ON test.records (sujet,predicat);")
session.execute("CREATE CUSTOM INDEX index_p ON test.records (sujet,objet);")
session.execute("CREATE CUSTOM INDEX index_p ON test.records (objet,predicat);")

dataFile = open("../Data/testdata.nt", "r")
data = dataFile.read().split('\n')
dataLength = len(data)
batch = BatchStatement()

#Insert triples
print("Inserting triples")
i = 0;
for line in progressbar(data):
    triple = line.split(' ')
    query = "INSERT INTO records (sujet, predicat, objet) VALUES (\'"+triple[0]+"\', \'"+triple[1]+"\', \'"+triple[2]+"\');"
    batch.add(SimpleStatement(query))

    if(i%1000==0 or i==dataLength-1):
        session.execute(batch)
        batch = BatchStatement()
        p = (i/dataLength)*100

    i += 1
print("done.")
