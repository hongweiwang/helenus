from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import random, time, md5, threading, sys, socket, os
from cassandraNode import CassandraNode
from helenus import Helenus
import numpy

if len(sys.argv) < 4:
    print 'usage: python client.py [option] [mean_interval] [nqueries]'
    sys.exit(1)

option = sys.argv[1]
mean_interval = float(sys.argv[2])
nqueries = int(sys.argv[3])
inverval_list = numpy.random.poisson(mean_interval, nqueries)

key_list = []
keyfile = 'keylist/keylist_' + sys.argv[3]

if not os.path.exists(keyfile):
    os.system('python genkey.py ' + sys.argv[3])
    print 'generate keyfile with size ' + sys.argv[3]

f = open(keyfile)
for line in f:
    key_list.append(line)
f.close()

nqueries = len(key_list)

IPs = ['155.98.39.126', '155.98.39.57', '155.98.39.93', '155.98.39.142']
host = socket.gethostname()
host = host[0:host.find('.')]
log = 'log/' + host + '_' + option + '_' + str(interval) + '_' + str(nqueries)
f = open(log, 'w')
helenus = Helenus(IPs, f)
keyspace = 'mykeyspace'
helenus.set_keyspace(keyspace)
table = 'mytable'

start = time.time()
if option == '1':
    for key in key_list:
        helenus.query_one_node(table, key)
        time.sleep(interval)
	#print 'interval'

if option == '2':
    for key in key_list:
        helenus.query_two_node(table, key)
        time.sleep(interval)

end = time.time()
elaps = end - start
f.close()

print host + ': complete testing in ' + str(elaps) + 's!'










