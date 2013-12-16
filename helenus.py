from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import random, time, md5, threading, sys, socket
from cassandraNode import CassandraNode

'''
if len(sys.argv) < 4:
	print 'usage: python helenus.py [log] [1|2] [nqueries]'
	sys.exit(1)

host = socket.gethostname()
host = host[0:host.find('.')]
log = host + '_' + sys.argv[1]
option = sys.argv[2]
nqueries = int(sys.argv[3])
f = open(log,'w')
'''
class Helenus():
	def __init__(self, IPs, f):
		self.nodes = []
		self.max = 100 * len(IPs)
		self.f = f
		for ip in IPs:
			machines = [ip]
			node = CassandraNode(machines)
			self.nodes.append(node)

	def create_keyspace(self, keyspace):
		for node in self.nodes:
			node.create_keyspace(keyspace)

	def set_keyspace(self, keyspace):
		for node in self.nodes:
			node.set_keyspace(keyspace)

	def create_table(self, table):
		for node in self.nodes:
			node.create_table(table)

	def drop_keyspace(self, keyspace):
		for node in self.nodes:
			node.drop_keyspace(keyspace)

	def get_nodes(self, key):
		number = self.hash(key)
		# print number
		node_index = number / 100
		next_node_index = node_index + 1
		if node_index == len(self.nodes) - 1:
			next_node_index = 0
		# print 'node_index: ' + str(node_index)
		# print 'next_node_index: ' + str(next_node_index)
		return node_index, next_node_index

	def insert(self, table, key, text):
		node_index, next_node_index = self.get_nodes(key)
		self.nodes[node_index].insert(table, key, text)
		self.nodes[next_node_index].insert(table, key, text)

	def query_one_node(self, table, key):
		#print 'query one node: ' + key[-6:]
		node_index, next_node_index = self.get_nodes(key)
		r = random.randint(0, 1)
		if r == 0:
			self.do_query(node_index, table, key)
		else:
			self.do_query(next_node_index, table, key)
		#print 'end query one node: ' + key[-6:]

	def query_two_node(self, table, key):
		#print 'query two node: ' + key[-6:]
		node_index, next_node_index = self.get_nodes(key)
		thread1 = threading.Thread(target=self.do_query, args=(node_index, table, key))
		thread2 = threading.Thread(target=self.do_query, args=(next_node_index, table, key))
		thread1.start()
		thread2.start()
		thread1.join()
		thread2.join()
		#print 'end query two node: ' + key[-6:]

	def do_query(self, node_index, table, key):
		start = time.time()
		self.nodes[node_index].query(table, key)
		end = time.time()
		elaps = (end - start) * 1000
		#print 'time: ' + str(elaps) + ' ms'
		self.f.write(str(elaps) + '\n')

	def show(self, table):
		for node in self.nodes:
			node.show(table)

	def show_count(self, table):
		for node in self.nodes:
			node.show_count(table)

	def hash(self, str):
		digest = md5.new(str).hexdigest()
		number = int(digest, 16)
		return number % self.max

def test():
	IPs = ['155.98.39.126', '155.98.39.57', '155.98.39.93', '155.98.39.142']
	# IPs = ['155.98.39.126']
	helenus = Helenus(IPs)
	keyspace = 'mykeyspace'
	#helenus.drop_keyspace(keyspace)
	#helenus.create_keyspace(keyspace)
	helenus.set_keyspace(keyspace)
	table = 'mytable'
	#helenus.create_table(table)

	key_size = 40
	value_size = 1000
	value = 'a' * value_size

	#for i in range(100000):
	#	key = 'a' * 35 + str(i).zfill(5)
	#	helenus.insert(table, key, value)

	#print 'insert complete!!!'
	# helenus.insert(table, 'mykey', 'myvalue')
	# helenus.query_one_node(table, 'mykey')
	# helenus.query_two_node(table, 'mykey')
	helenus.show_count(table)

def test_query(option):
	IPs = ['155.98.39.126', '155.98.39.57', '155.98.39.93', '155.98.39.142']
	helenus = Helenus(IPs)
	keyspace = 'mykeyspace'
	helenus.set_keyspace(keyspace)
	table = 'mytable'
	
	key_list = []
	for i in range(1):
		rnum = random.randint(0, 250000)
		key = 'a' * 35 + str(rnum).zfill(5)
		key_list.append(key)
	
	if option == '1':
		for key in key_list:
			helenus.query_one_node(table, key)
	if option == '2':
		for key in key_list:
			helenus.query_two_node(table, key)

#test_query('2')
#f.close()
