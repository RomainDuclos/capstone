from cassandra.cluster import Cluster
from hdt import HDTDocument

cluster = Cluster()
session = cluster.connect()

# Creating keyspace
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS testdata WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    }
    """
)

# on switch sur le bon KEYSPACE
session.set_keyspace('testdata')

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
exit()
# print("cardinality of { ?s ?p ?o }: %i" % 10)
for triple in zip( range(cardinality), triples):
    test = "INSERT INTO records (sujet, predicat, objet) VALUES (\'" + triple[1][0] + "\', \'" + triple[1][1] + "\', \'" + triple[1][2] + "\');"
    print(test)
    session.execute(test)

#En fait ici j'ai un truc INDEX qui se rajoute, donc je tape ssur triple[1] pour aller chercher ma data
# triple[0] = mon index
