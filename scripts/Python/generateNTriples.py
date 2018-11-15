import os
import sys

file = open("../Data/testdata.nt","w")

try:
    nbTriple = int(sys.argv[1])
except:
    print ("Wrong number of arguments\nUsage example: python generateTriples.py 3000000")
    exit(1)

for i in range(int(nbTriple/3)):
    file.write("a"+str(i)+" b "+"c"+"\n")
for i in range(int(nbTriple/3)):
    file.write("a "+"b"+str(i)+" c"+"\n")
for i in range(int(nbTriple/3)):
    file.write("a "+"b "+"c"+str(i)+"\n")

file.seek(0, os.SEEK_END)
file.seek(file.tell() - 1, os.SEEK_SET)
file.truncate()

file.close()
