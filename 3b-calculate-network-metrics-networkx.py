from __future__ import print_function
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
import networkx as nx
import pandas as pd

import sys


def result_to_pandas(res, name):
	df = pd.DataFrame.from_dict(res, orient="index")
	df.columns = [name]
	return df


def network_algorithms(g, dfs):
	print("Calculating network algorithms")

	# iterate over graph components
	for i in dfs:
		metrics = []

		# find all edges of the subgraph and only keep the "offer" relationship
		id_ = i.select("id").map(lambda a: a.id)
		ids = id_.collect()

		edges_ = g.edges.rdd.filter(lambda a: a.src in ids).toDF()
		df = edges_.select("src", "dst").toPandas()
		edge_list = [tuple(x) for x in df.values]

		# generate a networkx graph
		G = nx.Graph()
		G.add_edges_from(edge_list)

		# calculate several network metrics for the graph
		metrics.append(result_to_pandas(dict(nx.degree(G)), "degree"))

		metrics.append(result_to_pandas(nx.closeness_centrality(G), "closeness_centrality"))

		metrics.append(result_to_pandas(nx.betweenness_centrality(G), "betweenness_centrality"))

		metrics.append(result_to_pandas(nx.current_flow_closeness_centrality(G), "current_flow_closeness_centrality"))

		metrics.append(result_to_pandas(nx.current_flow_betweenness_centrality(G), "current_flow_betweenness_centrality"))

		metrics.append(result_to_pandas(nx.katz_centrality_numpy(G), "katz_centrality"))

		metrics.append(result_to_pandas(nx.load_centrality(G), "load_centrality"))

		metrics.append(result_to_pandas(nx.pagerank(G), "pagerank"))

		# TypeError: Cannot use scipy.linalg.eig for sparse A with k >= N - 1. Use scipy.linalg.eig(A.toarray()) or reduce k.
		# metrics.append(result_to_pandas(nx.eigenvector_centrality_numpy(G), "eigenvector_centrality"))

		# join network metrics into one graph
		res = pd.concat(metrics, axis=1, sort=False)
		res = res.reset_index(drop=False)
		res.rename(columns={"index": "id"}, inplace=True)
		print(res)

		# convert the result into spark dataframe
		spark_df = sqlContext.createDataFrame(res)

		# create or add to big dataframe that contains all components
		try:
			out = out.unionAll(spark_df)
		except NameError:
			out = spark_df

	return out


def build_graph(input_dir):
	print("Building graph")

	values = sqlContext.read.load(input_dir, format="parquet")

	company_nodes = values.select(F.col("company_nr").alias("id")).distinct()
	product_nodes = values.select(F.col("product").alias("id")).distinct()

	print("Companies " + str(company_nodes.count()))
	print("Products " + str(product_nodes.count()))

	nodes = company_nodes.unionAll(product_nodes)

	# create connection
	edges = values.withColumn("relationship", F.lit("offers"))
	edges = edges \
		.select(F.col("product").alias("dst"), F.col("company_nr").alias("src"), "relationship") \
		.distinct()

	return graphframes.GraphFrame(nodes, edges)


def find_subgraphs(graph):
	print("Finding connected components")

	cc = graph.connectedComponents()
	cc.persist(StorageLevel.DISK_ONLY)

	# split the df into subgraphs by component
	components = cc.select("component").distinct().flatMap(lambda x: x).collect()
	return [cc.where(cc["component"] == c) for c in components]


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
	s_conf.set("spark.executor.instances", "4")
	s_conf.set("spark.executor.memory", "2g")
	s_conf.set("spark.driver.memory", "2g")
	spark = SparkContext(conf=s_conf)
	spark.setLogLevel("ERROR")

	# needed here if running locally, in cluster it can be with other imports
	import graphframes

	sqlContext = SQLContext(spark)
	# needed for connected components
	# an existing directory!
	spark.setCheckpointDir("tmp/checkpoint")

	graph = build_graph(bipartite_location)

	dfs = find_subgraphs(graph)

	vertices_out = network_algorithms(graph, dfs)

	# output the data
	vertices_out.write.parquet(output_location)

	vertices_out.show()
	spark.stop()
