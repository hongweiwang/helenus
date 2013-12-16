import numpy, sys

if len(sys.argv) < 3:
    print 'usage: python genpoisson.py [mean] [number]'
    sys.exit(1)

mean = float(sys.argv[1])
number = int(sys.argv[2])
f = open('poisson/' + str(mean) + '_' + str(number), 'w')

s = numpy.random.poisson(mean, number)

for i in range(number):
    f.write(str(s[i]) + '\n')

f.close()
