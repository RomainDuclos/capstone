import sys
import matplotlib.pyplot as plt

data = open(sys.argv[1], 'r')

plt.title('Temps pour executer une requete depuis un etat donne')
plt.xlabel('Requete')
plt.ylabel('Temps (secondes)')

x = float(0)

for line in data:
    y = float(line.split(' ')[0])
    if(y):
        plt.plot(x, y, 'b+')
        x=x+1

plt.show()
