import networkx as nx
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import sys

SparkContext.setSystemProperty('spark.executor.memory', '2g')
sparkconf = SparkConf()
sparkconf.setAppName("Build bipartite graphs")
spark = SparkContext(conf=sparkconf)
spark.setLogLevel('ERROR')
sqlContext = SQLContext(spark)

edges = sqlContext.read.load("edges3", format="parquet").collect()
vertices = sqlContext.read.load("vertices3", format="parquet").collect()

# bad = "<https://graph.ir.ee/organizations/ee-10510593>"
# good = "<https://graph.ir.ee/organizations/ee-10730372>"
connections = []
for row in edges:
	# source = row.src.replace(bad, good)
	# target = row.dst.replace(bad, good)
	connections.append((row.src, row.dst))

G = nx.Graph()
G.add_edges_from(connections)
nx.write_graphml(G, 'graph-all-comps.graphml')
sys.exit(1)


# generate out of all vertices that are two steps from targets
# targets = set(["sku:83204", "sku:84897", "sku:83241", "sku:xu300", "sku:14630", "sku:55901", "sku:57204"])
targets = set(["sku:56872"])

keep = []

next_targets = set()
for i in connections:
	if i[0] in targets:
		next_targets.add(i[1])
	if i[1] in targets:
		next_targets.add(i[0])

for i in connections:
	if i[0] in next_targets or i[1] in next_targets:
		keep.append(i)

print keep

G = nx.Graph()
G.add_edges_from(keep)
nx.write_graphml(G, 'sub.graphml')
