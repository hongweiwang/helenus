import random, sys

if len(sys.argv) < 2:
    print 'usage: python genkey.py [number]'
    sys.exit(1)

number = int(sys.argv[1])
f = open('keylist/keylist_' + str(number), 'w')

for i in range(number):
	rnum = random.randint(0, 250000)
	key = 'a' * 35 + str(rnum).zfill(5)
	f.write(key + '\n')
	f.flush()

f.close()
