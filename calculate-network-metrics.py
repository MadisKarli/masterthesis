from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

import graphframes

if __name__ == "__main__":
	spark = SparkContext()
	spark.setLogLevel("ERROR")

	sql_context = SQLContext(spark)

	# values = sql_context.read.load("/home/madis/IR/thesis/bipartite-all", format="parquet")
	# values = sql_context.read.load("/home/madis/IR/thesis/bipartite-sku-matched", format="parquet")
	values = sql_context.read.load("/home/madis/IR/thesis/parquets/bipartite-sku-only-sku-matches", format="parquet")
	# values = sql_context.read.load("/home/madis/IR/thesis/bipartite-all-filtered", format="parquet")

	company_nodes = values.select(F.col("company_nr").alias("id")).distinct()
	product_nodes = values.select(F.col("product").alias("id")).distinct()

	# dont delete
	print("Companies " + str(company_nodes.count()))
	print("Products " + str(product_nodes.count()))

	# print(company_nodes.collect())
	# print(product_nodes.collect())

	nodes = company_nodes.unionAll(product_nodes)

	edges = values.withColumn("relationship", F.lit("minedFrom"))
	edges = edges.select(F.col("company_nr").alias("src"), F.col("product").alias("dst"), "relationship").distinct()

	g = graphframes.GraphFrame(nodes, edges)

	# this is how we can search the graph
	# it could be used to find connections in the graphs
	# should be checked against what we already have
	# https://graphframes.github.io/user-guide.html
	# search = g.find("(a)-[]->(b)")
	# #[5260B024]
	# search.filter('b.id = "[EHA6041XOK]"').show()
	#search.show(10, False)


	#result = g.connectedComponents()
	#result.select("id", "component").orderBy("component").show()

	print("Num Vertices: ")
	print(g.vertices.count())
	print("Num (suitable) Edges: ")
	print(g.edges.count())

	print("TopDegrees: ")
	vertices_out = g.degrees
	#vertices_with_degree.sort(F.desc("degree")).show(20, False)



	print("PageRank: ")
	pagerank = g.pageRank(resetProbability=0.15, maxIter=3)
	#pagerank.vertices.show(50, False)
	#pagerank.edges.sort(F.desc("weight")).show(20, False)
	# todo maybe it could be made more efficient using with column
	# tried it but did not get a column out of a dataframe
	vertices_out = vertices_out.join(pagerank.vertices, "id")
	vertices_out.show(50, False)

	# output the data
	#g.vertices.write.parquet("vertices")
	#g.edges.write.parquet("edges")

	vertices_out.write.parquet("network-metrics")

	#g.edges.show(50, False)
	#g.vertices.show(20, False)

	spark.stop()
