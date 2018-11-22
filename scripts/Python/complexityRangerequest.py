from cassandra.cluster import Cluster
import time
import statistics

GlobalStart = time.time()

cluster = Cluster(['172.16.134.144', '172.16.134.142', '172.16.134.143'])
session = cluster.connect()

#variables
nbTotalLignesTheorique = 3000000
nbRange = 10000
nbIter = nbTotalLignesTheorique/nbRange
listTimers = []

# on switch sur le bon KEYSPACE


# SPO

def queryComplexity( queryParam , name, tokenParam):
    print(name)
    for i in range(nbIter):
        if i==0 :
            query = queryParam + " limit " + str(nbRange) + ";"
        else :
            query = queryParam + " and token(" + tokenParam + ") > " + str(token) + " limit " + str(nbRange) + ";"
            print(query)
        start = time.time()
        maRange = session.execute(query)
        if(maRange):
            token = maRange[-1][-1]
            end = time.time()
            #liste des temps pour stats
            listTimers.append(end-start)
        else:
            break;
        #On va chercher le dernier token et on le charge
        # le ifelse est pour eviter que ca plante
        # print(token)
    # stats
    i=0
    for timer in listTimers:
        i = i+1
        monTimer = str(timer)
        while(len(monTimer)<15):
            monTimer += "0"
        print(monTimer + " seconds")

    GlobalEnd = time.time()

    print("total de " + str(i) + " tokens lus sur des ranges de " + str(nbRange) + " lignes")
    print("Temps global d'execution du programme : " + str(GlobalEnd-GlobalStart) + " seconds")
    print("______________________________")


session.set_keyspace('pks')
# SPO
query = "SELECT sujet, predicat, objet, token(sujet) FROM records WHERE sujet='a' and predicat='b' and objet='c1'"
queryComplexity( query, "SPO", "sujet")
# S..
query = "SELECT sujet, predicat, objet, token(sujet) FROM records WHERE sujet='a'"
queryComplexity( query, "S..", "sujet")
# SP.
query = "SELECT sujet, predicat, objet, token(sujet) FROM records WHERE sujet='a' and predicat='b'"
queryComplexity( query, "SP.", "sujet")

# session.set_keyspace('pkp')
#
# #.P.
# query = "SELECT * FROM records WHERE predicat='b';"
# queryComplexity( query, ".P.")
# # #.PO
# query = "SELECT * FROM records WHERE predicat='b' and objet='c';"
# queryComplexity( query, ".PO")
#
# session.set_keyspace('pko')
#
# # ..O
# query = "SELECT * FROM records WHERE objet='c';"
# queryComplexity( query, "..O")
# # S.O
# query = "SELECT * FROM records WHERE sujet='a' and objet='c';"
# queryComplexity( query, "S.O")
