import os, sys

if len(sys.argv) < 4:
    print 'usage: python experiment.py [option] [load] [nqueries]'
    sys.exit(1)

# count the total number of throughput
dir = 'throughput/'
throughput = 0.0
for file in os.listdir(dir):
    f = open(dir + file)
    for line in f:
        throughput = throughput + float(line)
    f.close()

print 'total throughput: ' + str(throughput) + ' queries/s'

option = sys.argv[1]
load = float(sys.argv[2])
nqueries = sys.argv[3]

client_throughput = load * throughput / 10.0
print 'each client throughput: ' + str(client_throughput) + ' queries/s'
mean_interval = 1.0 / client_throughput
print 'each client mean interval: ' + str(mean_interval) + 's'

# run the client
os.system('./cmd.sh python client.py ' + option + ' ' + str(mean_interval) + ' ' + nqueries)
print 'experiment complete!'
