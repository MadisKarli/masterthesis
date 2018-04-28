import networkx as nx
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

SparkContext.setSystemProperty('spark.executor.memory', '2g')
sparkconf = SparkConf()
sparkconf.setAppName("Build bipartite graphs")
spark = SparkContext(conf=sparkconf)
spark.setLogLevel('ERROR')
sqlContext = SQLContext(spark)

edges = sqlContext.read.load("/home/madis/IR/thesis/parquets/bipartite-all-filtered-edges", format="parquet").collect()
vertices = sqlContext.read.load("/home/madis/IR/thesis/parquets/bipartite-all-filtered-vertices", format="parquet").collect()

# enumberate
# vertices_dict = {}
# for idx, row in enumerate(vertices):
# 	vertices_dict[row.id] = idx

connections = []
for row in edges:
	# src = vertices_dict[row.src]
	# dst = vertices_dict[row.dst]
	connections.append((row.src, row.dst))

G = nx.Graph()
G.add_edges_from(connections)
nx.write_graphml(G, 'bipartite-all-filtered.graphml')
