from __future__ import print_function
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import Row


import sys, os, time

#https://github.com/graphframes/graphframes/issues/256
from pyspark.sql.types import StructType

from lib.aggregate_messages import AggregateMessages as AM


def eccentricity(dfs):
	# Eccentricity is maximum distance between a vertex to furthest vertex
	# http://mathworld.wolfram.com/GraphEccentricity.html
	# Since we have a disconnected graph, all eccentricities are infinity
	# but R's igraph ignores that, so lets say that if two vertices are not connected then their eccentricity is 0
	# Algorithm
	# 1. find connected components
	# 2. calculate shortest paths from every node to every other node (this can also be used for betweenness)
	# 3. find max shortest path for every node, if no shortest path then set it to 0

	for i in dfs:
		# calculate shortest paths only for company nodes
		id_ = i.select("id").map(lambda a: a.id)
		ids = id_.collect()

		# create a subgraph for component
		edges_ = g.edges.rdd.filter(lambda a: a.dst in ids or a.src in ids).toDF()
		vertices_ = g.vertices.rdd.filter(lambda a: a.id in ids).toDF()

		g_sub = graphframes.GraphFrame(vertices_, edges_)

		# calculate shortest paths
		shortest_paths = g_sub.shortestPaths(landmarks=ids)# possible optimization: use different landmarks

		# find out only the longest shortest paths
		ecc = shortest_paths.map(lambda a: Row(id=a.id, eccentricity=max(a.distances.values()))).toDF()

		# collect all Eccentricities into one df
		try:
			tmp = tmp.unionAll(ecc)
		except NameError:
			tmp = ecc

	return tmp


def betweenness(dfs):
	for i in dfs:
		# calculate shortest paths only for company nodes
		id_ = i.select("id").map(lambda a: a.id)
		ids = id_.collect()

		# create a subgraph for component
		edges_ = g.edges.rdd.filter(lambda a: a.dst in ids or a.src in ids).toDF()
		vertices_ = g.vertices.rdd.filter(lambda a: a.id in ids).toDF()

		g_sub = graphframes.GraphFrame(vertices_, edges_)
		g_sub.persist(StorageLevel.DISK_ONLY)

		# even though SPARKSQL does allow wildcards etc, they only return ONLY the shortest path discovered for every vertex
		# calculate shortest paths
		# O(n^2) :(
		for j in enumerate(ids[:-1]):
			for k in ids[j[0]+1:]:
				print(j[1], k)
				path = g_sub.bfs("id='" + j[1] + "'", "id='" + k + "'")

				# select all v's. colnames: from, (e)dges, (v)ertices, to
				names = [x for x in path.columns if 'v' in x]

				if len(names) > 0:
					vs = path.select(names).flatMap(list).toDF().groupBy("id").agg(F.expr("count(*) as betweenness"))
					try:
						tmp = tmp.unionAll(vs)
					except NameError:
						tmp = vs
					# vs = path.select(names)
					# vertices_on_path = vs.flatMap(list).map(lambda a: a.id).collect()
					#
					# for v in vertices_on_path:
					# 	try:
					# 		out[v] = out[v] + 1
					# 	except KeyError:
					# 		out[v] = 1
					#
					# print(out)
					break
			break
	tmp.show()
	return tmp


if __name__ == "__main__":

	if len(sys.argv) < 3:
		print("\nUsage:")
		print("spark-submit --packages graphframes:graphframes:0.5.0-spark1.6-s_2.10 "
			  "3-calculate-network-metrics.py location_of_bipartite_dataframe output_location ")
		print("\n Example")
		print("spark-submit --packages graphframes:graphframes:0.5.0-spark1.6-s_2.10 "
			  "3-calculate-network-metrics.py "
			  "/home/madis/IR/thesis/parquets/bipartite-sku-only-sku-matches "
			  "network-metrics")
		sys.exit(1)
	else:
		bipartite_location = sys.argv[1]
		output_location = sys.argv[2]

	s_conf = SparkConf()
	s_conf.set("spark.executor.instances", "1")
	s_conf.set("spark.executor.memory", "14g")
	s_conf.set("spark.driver.memory", "2g")
	s_conf.set("spark.sql.broadcastTimeout", "36000")
	spark = SparkContext(conf=s_conf)
	spark.setLogLevel("ERROR")

	# needed here if running locally, in cluster it can be with other imports
	import graphframes

	# needed for connected components
	# an existing directory!
	spark.setCheckpointDir("tmp/checkpoint")

	sqlContext = SQLContext(spark)

	values = sqlContext.read.load(bipartite_location, format="parquet")

	company_nodes = values.select(F.col("company_nr").alias("id")).distinct()
	product_nodes = values.select(F.col("product").alias("id")).distinct()

	print("Companies " + str(company_nodes.count()))
	print("Products " + str(product_nodes.count()))

	nodes = company_nodes.unionAll(product_nodes)

	# create two connections
	# company to product - minedFrom
	# product to company - belongsTo
	edges_to_product = values.withColumn("relationship", F.lit("minedFrom"))
	edges_to_product = edges_to_product \
		.select(F.col("company_nr").alias("src"), F.col("product").alias("dst"), "relationship") \
		.distinct()

	# as we need pagerank for companies then we create another df so that it would be directed
	edges_from_product = values.withColumn("relationship", F.lit("offers"))
	edges_from_product = edges_from_product \
		.select(F.col("product").alias("dst"), F.col("company_nr").alias("src"), "relationship") \
		.distinct()

	edges = edges_from_product.unionAll(edges_to_product)

	g = graphframes.GraphFrame(nodes, edges)

	# For each vertex, find the average neighbour degree
	# fist calculate the degree
	print("Calculating Degree")
	vertices_out = g.degrees

	gx = graphframes.GraphFrame(vertices_out, edges)

	print("Calculating Nearest Neighbour Degree")
	msgToSrc = None
	msgToDst = AM.src["degree"]
	nnd = gx.aggregateMessages(
		F.avg(AM.msg).alias("nearest-neighbour-degree"),
		sendToSrc=msgToSrc,
		sendToDst=msgToDst)

	vertices_out = vertices_out.join(nnd, "id")

	print("Calculating Average Nearest Neigbour Degree")
	vertices_out = vertices_out \
		.join(vertices_out.groupBy(vertices_out.degree).avg("nearest-neighbour-degree"), "degree") \
		.select(F.col("id"), F.col("degree"), F.col("nearest-neighbour-degree"), F.col("avg(nearest-neighbour-degree)").alias("annd"))

	print("Calculating Eccentricity")

	# Eccentricity is maximum distance between a vertex to all other vertices
	cc = g.connectedComponents()
	cc.persist(StorageLevel.DISK_ONLY)

	# split the df into subgraphs by component
	components = cc.select("component").distinct().flatMap(lambda x: x).collect()
	dfs = [cc.where(cc["component"] == c) for c in components]

	# todo we could also create the graphs here, and use it as a list

	vertices_out = vertices_out.join(eccentricity(dfs), "id")
	print("Calculating Betweenness centrality")

	# Betweenness centrality shows how many shortest paths pass through a graph
	vertices_out = vertices_out.join(betweenness(dfs), "id", "outer")

	print("Calculating PageRank")
	pagerank = g.pageRank(resetProbability=0.15, maxIter=3)

	# pagerank.vertices.show(50, False)
	# pagerank.edges.sort(F.desc("weight")).show(5000, False)

	# todo maybe could be made more efficient using with column instead of join
	# tried it but did not get a column out of a dataframe
	vertices_out = vertices_out.join(pagerank.vertices, "id")
	# vertices_out.sort(F.desc("degree")).show(50, False)

	# output the data
	vertices_out.write.parquet(output_location)

	# g.edges.show(50, False)
	# g.vertices.show(50, False)

	vertices_out.show()
	spark.stop()
