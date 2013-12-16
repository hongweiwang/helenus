from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import random, time, md5, threading, sys, socket, os
from cassandraNode import CassandraNode
from helenus import Helenus

if len(sys.argv) < 2:
    print 'usage: python throughput.py [nqueries]'
    sys.exit(1)

host = socket.gethostname()
host = host[0:host.find('.')]

key_list = []
keyfile = 'keylist/keylist_' + sys.argv[1]

if not os.path.exists(keyfile):
    os.system('python genkey.py ' + sys.argv[1])
    print 'generate keyfile with size ' + sys.argv[1]

f = open(keyfile)
for line in f:
    key_list.append(line)

nqueries = len(key_list)

IPs = ['155.98.39.126', '155.98.39.57', '155.98.39.93', '155.98.39.142']
f = open('log/throughput', 'w')
helenus = Helenus(IPs, f)
keyspace = 'mykeyspace'
helenus.set_keyspace(keyspace)
table = 'mytable'

start = time.time()
for key in key_list:
    helenus.query_one_node(table, key)
end = time.time()
elaps = end - start
throughput = float(nqueries) / float(elaps) * float(1000)
# print 'throughput: ' + str(throughput)
f_th = open('throughput/' + host + '_throughput', 'w')
f_th.write(str(throughput))
f_th.close()
f.close()
print 'host: ' + host + ' complete in ' + str(elaps) + 's'
