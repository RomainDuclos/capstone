import sys
import matplotlib.pyplot as plt
import matplotlib as mpl

data = open(sys.argv[1], 'r')

mpl.rcParams.update({'font.size':22})
plt.title('Time for a same query to retrieve the next result from a preceding index')
# plt.title('Temps d\'exécution d\'une requête reprenant depuis un indice donné')
plt.xlabel('Index')
plt.ylabel('Time (millisecondes)')

x = float(0)
for line in data:
    y = float(line.split(' ')[0])
    if(y):
        plt.plot(x, y*1000, 'b+')
        x=x+1



plt.show()
