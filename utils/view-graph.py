import networkx as nx
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

SparkContext.setSystemProperty('spark.executor.memory', '2g')
sparkconf = SparkConf()
sparkconf.setAppName("Build bipartite graphs")
spark = SparkContext(conf=sparkconf)
spark.setLogLevel('ERROR')
sqlContext = SQLContext(spark)

edges = sqlContext.read.load("/home/madis/IR/thesis/parquets/edges", format="parquet").collect()
vertices = sqlContext.read.load("/home/madis/IR/thesis/parquets/vertices", format="parquet").collect()

# enumberate
vertices_dict = {}
for idx, row in enumerate(vertices):
	vertices_dict[idx] = row.id

connections = []
for row in edges:
	src = [filter(lambda x: vertices_dict[x] == row.src, vertices_dict), [None]][0][0]
	dst = [filter(lambda x: vertices_dict[x] == row.dst, vertices_dict), [None]][0][0]
	connections.append((row.src, row.dst))

G=nx.Graph()
G.add_edges_from(connections)
nx.write_graphml(G,'skugraph.graphml')
