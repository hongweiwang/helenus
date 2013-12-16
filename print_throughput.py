import os

dir = 'throughput/'
for file in os.listdir(dir):
	f = open(dir + file)
	for line in f:
		print line
	f.close()
