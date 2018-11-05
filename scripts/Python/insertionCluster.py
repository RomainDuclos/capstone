from cassandra.cluster import Cluster
from hdt import HDTDocument
from cassandra.policies import DCAwareRoundRobinPolicy

cluster = Cluster(
    ['172.16.134.141', '172.16.134.142', '172.16.134.143'],
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'))

session = cluster.connect()


# Dans ce script on fait une insertion sur une primary key complexe afin de pouvoir faire des requetes avec nos tokens derriere

# Creating keyspace
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS testcluster WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    }
    """
)

# on switch sur le bon KEYSPACE
session.set_keyspace('testcluster')

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

# Load an HDT file. Missing indexes are generated automatically
document = HDTDocument("../Data/wordnet31.hdt")

# Fetch all triples that matches { ?s ?p ?o }
# Use empty strings ("") to indicates variables
(triples, cardinality) = document.search_triples("", "", "")

print(cardinality)

i=0
# print("cardinality of { ?s ?p ?o }: %i" % 10)
for triple in zip( range(cardinality), triples):
    i = i+1
    test = "INSERT INTO records (sujet, predicat, objet) VALUES ($$" + triple[1][0].replace("$", "\$") + "$$, $$" + triple[1][1].replace("$", "\$") + \
    "$$, $$" + triple[1][2].replace("$", "\$") + "$$ );"
    print(test)
#    if i>1:
#        exit(1)
    session.execute(test)
    print(i)

print("done")
#En fait ici j'ai un truc INDEX qui se rajoute, donc je tape ssur triple[1] pour aller chercher ma data
# triple[0] = mon index
