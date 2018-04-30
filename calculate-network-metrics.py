from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

import graphframes

if __name__ == "__main__":
	spark = SparkContext()
	spark.setLogLevel("ERROR")

	sql_context = SQLContext(spark)

	values = sql_context.read.load("/home/madis/IR/thesis/parquets/bipartite-sku-only-sku-matches", format="parquet")
	# values = sql_context.read.load("hdfs://ir-hadoop1/user/madis/bipartite-all-sku-only", format="parquet")

	company_nodes = values.select(F.col("company_nr").alias("id")).distinct()
	product_nodes = values.select(F.col("product").alias("id")).distinct()

	print("Companies " + str(company_nodes.count()))
	print("Products " + str(product_nodes.count()))

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
	edges_from_product = values.withColumn("relationship", F.lit("belongsTo"))
	edges_from_product = edges_from_product\
		.select(F.col("product").alias("dst"), F.col("company_nr").alias("src"), "relationship")\
		.distinct()

	edges = edges_from_product.unionAll(edges_to_product)

	g = graphframes.GraphFrame(nodes, edges)

	# print("Num Vertices: ")
	# print(g.vertices.count())
	# print("Num (suitable) Edges: ")
	# print(g.edges.count())

	print("TopDegrees: ")
	vertices_out = g.degrees
	vertices_out.sort(F.desc("degree")).show(50, False)

	print("PageRank: ")
	pagerank = g.pageRank(resetProbability=0.15, maxIter=3)
	# pagerank.vertices.show(50, False)
	# pagerank.edges.sort(F.desc("weight")).show(5000, False)

	# todo maybe could be made more efficient using with column instead of join
	# tried it but did not get a column out of a dataframe
	vertices_out = vertices_out.join(pagerank.vertices, "id")
	vertices_out.show(50, False)

	# output the data
	vertices_out.write.parquet("network-metrics")

	g.edges.show(50, False)
	g.vertices.show(50, False)

	spark.stop()
