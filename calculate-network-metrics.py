from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

import sys

import graphframes
#https://github.com/graphframes/graphframes/issues/256
from lib.aggregate_messages import AggregateMessages as AM

if __name__ == "__main__":
	if len(sys.argv) < 3:
		print("\nUsage:")
		print("--packages graphframes:graphframes:0.5.0-spark1.6-s_2.10 "
			  "calculate-network-metrics.py location_of_bipartite_dataframe output_location ")
		print("\n Example")
		print("spark-submit --packages graphframes:graphframes:0.5.0-spark1.6-s_2.10 "
			  "calculate-network-metrics.py "
			  "/home/madis/IR/thesis/parquets/bipartite-sku-only-sku-matches"
			  "network-metrics")
		sys.exit(1)
	else:
		bipartite_location = sys.argv[1]
		output_location = sys.argv[2]

	spark = SparkContext()
	spark.setLogLevel("ERROR")

	sql_context = SQLContext(spark)

	values = sql_context.read.load(bipartite_location, format="parquet")

	company_nodes = values.select(F.col("company_nr").alias("id")).distinct()
	product_nodes = values.select(F.col("product").alias("id")).distinct()

	# print("Companies " + str(company_nodes.count()))
	# print("Products " + str(product_nodes.count()))

	# print(company_nodes.collect())
	# print(product_nodes.collect())

	nodes = company_nodes.unionAll(product_nodes)

	# create two connections
	# company to product - minedFrom
	# product to company - belongsTo
	# todo ask peep if directed graph is ok or do we want undirected one?
	edges_to_product = values.withColumn("relationship", F.lit("minedFrom"))
	edges_to_product = edges_to_product\
		.select(F.col("company_nr").alias("src"), F.col("product").alias("dst"), "relationship")\
		.distinct()

	# as we need pagerank for companies then we create another df so that it would be directed
	edges_from_product = values.withColumn("relationship", F.lit("offers"))
	edges_from_product = edges_from_product\
		.select(F.col("product").alias("dst"), F.col("company_nr").alias("src"), "relationship")\
		.distinct()

	edges = edges_from_product.unionAll(edges_to_product)

	g = graphframes.GraphFrame(nodes, edges)

	# edges.write.parquet("thesis/edges3")
	# nodes.write.parquet("thesis/vertices3")

	# print("Num Vertices: ")
	# print(g.vertices.count())
	# print("Num (suitable) Edges: ")
	# print(g.edges.count())

	# For each vertex, find the average neighbour degree
	# fist calculate the degree
	print("TopDegrees: ")
	vertices_out = g.degrees
	# vertices_out.sort(F.desc("degree")).show(50, False)

	gx = graphframes.GraphFrame(vertices_out, edges)

	print("Nearest Neighbour Degree")
	msgToSrc = None
	msgToDst = AM.src["degree"]
	nnd = gx.aggregateMessages(
		F.avg(AM.msg).alias("nearest-neighbour-degree"),
		sendToSrc=msgToSrc,
		sendToDst=msgToDst)

	vertices_out = vertices_out.join(nnd, "id")

	print("Average Nearest Neigbour Degree")
	vertices_out = vertices_out \
		.join(vertices_out.groupBy(vertices_out.degree).avg("nearest-neighbour-degree"), "degree") \
		.select(F.col("id"), F.col("degree"), F.col("nearest-neighbour-degree"), F.col("avg(nearest-neighbour-degree)").alias("annd"))

	print("PageRank: ")
	pagerank = g.pageRank(resetProbability=0.15, maxIter=3)
	# pagerank.vertices.show(50, False)
	# pagerank.edges.sort(F.desc("weight")).show(5000, False)

	# todo maybe could be made more efficient using with column instead of join
	# tried it but did not get a column out of a dataframe
	vertices_out = vertices_out.join(pagerank.vertices, "id")
	vertices_out.show(50, False)

	# output the data
	vertices_out.write.parquet(output_location)

	# g.edges.show(50, False)
	# g.vertices.show(50, False)

	spark.stop()
