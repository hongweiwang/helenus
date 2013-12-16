#!/usr/bin/env python

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import random, time

class CassandraNode():
	def __init__(self, machines):
		self.machines = machines
		self.cluster = Cluster(machines)
		self.session = self.cluster.connect()

	def create_keyspace(self, keyspace):
		print 'creating keyspace...'
		self.session.execute("""
			CREATE KEYSPACE %s
			WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
		""" % keyspace)

	def set_keyspace(self, keyspace):
		#print 'setting keyspace...'
		self.session.set_keyspace(keyspace)

	def create_table(self, table):
		print 'creating table...'
		self.session.execute("""
			CREATE TABLE %s (
			key text PRIMARY KEY,
			col text
			)
		""" % table)

	def insert(self, table, key, text):
		statement = """
			INSERT INTO %s (key, col)
			VALUES (\'%s\', \'%s\')
		""" % (table, key, text)
		# print 'insert (\'%s\', \'%s\')' % (key, text) 
		self.session.execute(statement)

	def query(self, table, key):
		statement = "SELECT * FROM %s where key = \'%s\'" % (table, key)
		# print "query key = '" + key + "'"
		response = self.session.execute(statement)
		#print response

	def show(self, table):
		print 'show from node: ' + self.machines[0]
		statement = "SELECT * FROM %s" % table
		# print "query key = '" + key + "'"
		response = self.session.execute(statement)
		print response

	def show_count(self, table):
		print 'node: ' + self.machines[0] + ' has keys: '
		statement = "SELECT count(*) FROM %s" % table
		# print "query key = '" + key + "'"
		response = self.session.execute(statement)
		print response

	def drop_keyspace(self, keyspace):
		rows = self.session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
		if keyspace in [row[0] for row in rows]:
			print "dropping keyspace " + keyspace
			self.session.execute("DROP KEYSPACE " + keyspace)


# if __name__ == "__main__":
# 	machines = ['127.0.0.1']
# 	node = CassandraNode(machines)
# 	keyspace = 'mykeyspace'
# 	table = 'mytable'
# 	node.drop_keyspace(keyspace)
# 	# node.create_keyspace(keyspace)
# 	node.set_keyspace(keyspace)
# 	# node.create_table(table)

# 	key_size = 40
# 	value_size = 1000

# 	value = 'a' * value_size

	# for i in range(100000):
	# 	key = 'a' * 35 + str(i).zfill(5)
	# 	node.insert(table, key, value)

	# print 'insert complete!!!'

	# f_out = open('log', 'w')
	# for i in range(10):
	# 	rnum = random.randint(0, 99999)
	# 	print rnum
	# 	key = 'a' * 35 + str(rnum).zfill(5)
	# 	start = time.time()
	# 	node.query(table, key)
	# 	end = time.time()
	# 	query_time = (end - start) * 1000
	# 	f_out.write(str(query_time) + '\n')
	# 	f_out.flush()
	# f_out.close()










