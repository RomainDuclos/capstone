import sys
import matplotlib.pyplot as plt

data = open(sys.argv[1], 'r')

plt.title("Temps d acces aux tokens en seconde")
plt.xlabel('token')
plt.ylabel('seconde')

x = float(0)

for line in data:
    y = float(line.split(' ')[0])
    if(y):
        plt.plot(x, y, 'b+')
        x=x+1

plt.show()
