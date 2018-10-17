from cassandra.cluster import Cluster
from hdt import HDTDocument


# Dans ce script on fait une insertion sur une primary key complexe afin de pouvoir faire des requetes avec nos tokens derrière



cluster = Cluster()
session = cluster.connect()

# Creating keyspace
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS jekasstout WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    }
    """
)

# on switch sur le bon KEYSPACE
session.set_keyspace('jekasstout')

session.execute(
    """
    CREATE TABLE IF NOT EXISTS records (
    sujet text,
	predicat text,
	objet text,
    id int,
	PRIMARY KEY ((sujet, predicat, objet), id)
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

# Load an HDT file. Missing indexes are generated automatically
document = HDTDocument("tests/data/test.hdt")

# Fetch all triples that matches { ?s ?p ?o }
# Use empty strings ("") to indicates variables
(triples, cardinality) = document.search_triples("", "", "")

print(cardinality)

i = 0

# print("cardinality of { ?s ?p ?o }: %i" % 10)
for triple in zip( range(cardinality), triples):
    test = "INSERT INTO records (sujet, predicat, objet, id) VALUES (\'" + triple[1][0] + "\', \'" + triple[1][1] + "\', \'" + triple[1][2] + "\' , " + str(i) + " );"
    i = i+1
#    print(test)
    session.execute(test)

print("done")
#En fait ici j'ai un truc INDEX qui se rajoute, donc je tape ssur triple[1] pour aller chercher ma data
# triple[0] = mon index
