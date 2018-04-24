from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

import graphframes

if __name__ == "__main__":
	spark = SparkContext()
	spark.setLogLevel("ERROR")

	sql_context = SQLContext(spark)

	values = sql_context.read.load("bipartite", format="parquet")

	company_nodes = values.select(F.col("company_nr").alias("id")).distinct()
	product_nodes = values.select(F.col("product").alias("id")).distinct()

	nodes = company_nodes.unionAll(product_nodes)

	edges = values.withColumn("relationship", F.lit("minedFrom"))
	edges = edges.select(F.col("company_nr").alias("src"), F.col("product").alias("dst"), "relationship")

	g = graphframes.GraphFrame(nodes, edges)

	# this is how we can search the graph
	# it could be used to find connections in the graphs
	# should be checked against what we already have
	# https://graphframes.github.io/user-guide.html
	# search = g.find("(a)-[e]->(b)")
	# search.show(1000, False)

	result = g.connectedComponents()
	result.select("id", "component").orderBy("component").show()

	print("Num Vertices: ")
	print(g.vertices.count())
	print("Num (suitable) Edges: ")
	print(g.edges.count())

	print("TopDegrees: ")
	g.degrees.sort(F.desc("degree")).show(20, False)

	print("")

	# print("PageRank: ")
	# results2 = g.pageRank(resetProbability=0.15, maxIter=3)
	#
	# results2.edges.sort(F.desc("weight")).show(20, False)

	spark.stop()
