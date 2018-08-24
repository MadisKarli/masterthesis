from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import Row

from pyspark.sql import functions as F

import csv
import sys


def filterEdges(col):
	allowed = ["<http://graph.ir.ee/media/usedBy>",
			   "<http://www.w3.org/1999/xhtml/microdata#item>",
			   "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
			   ]

	if col in allowed:
		return True
	return False


def filterCompanies(col):
	allowed = ["<http://graph.ir.ee/media/usedBy>",
			   "<http://www.w3.org/1999/xhtml/microdata#item>",
			   "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
			   ]

	if col in allowed:
		return True
	return False


def filterProducts(col):
	if "http://schema.org/Product" in col:
		return True
	return False


def split_into_triples(row):
	csv.field_size_limit(100000000)
	row = row.encode('utf-8')
	res = [x for x in csv.reader([row], delimiter=' ')][0]
	if len(res) == 4:
		if res[3] == ".":
			return res


def remove_language_strings(row):
	object = row[2]

	parts = object.split("@")

	language_tag = parts[-1]

	# case 1: @et or @fi at the end
	if len(language_tag) == 2:
		object = "@".join(parts[:-1]).strip('"')
	# case 2: @et-ee at the end
	elif len(language_tag) == 5:
		if language_tag[2] == "-":
			object = "@".join(parts[:-1]).strip('"')

	return [row[0], row[1], object]


# todo actually we could steal the sku from here
def remove_empty_skus(row):
	predicate = row[1]
	object = row[2]

	if predicate != "<http://schema.org/Product/sku>":
		return True

	if object == "Null":
		return False

	if object == "N/A":
		return False

	return True


def add_sku_relationship(row):
	subject = row[0]
	predicate = row[1]
	object = row[2]

	if predicate == "<http://schema.org/Product/sku>":
		# if we have multiple SKUs then use only last one
		parts = object.split(" / ")
		if len(parts) > 1:
			object = parts[-1]
		return [subject, "<http://schema.org/Product/sameAs>", "sku:" + object]


if __name__ == "__main__":
	if len(sys.argv) < 5:
		print("Usage:")
		print("bipartite-graphs.py location_of_microdata_triples location_of_company_triples "
			  "output_all_location output_only_matches_location")
		print("\n Example")
		print("python bipartite-graphs.py "
			  "/home/madis/IR/data/microdata_from_warcs/skumatch "
			  "/home/madis/IR/data/microdata_from_warcs/skumatch-companies "
			  "bipartite-all "
			  "bipartite-all-sku-only")
		sys.exit(1)
	else:
		microdata_triples_location = sys.argv[1]
		company_triples_location = sys.argv[2]
		output_all_location = sys.argv[3]
		output_filtered_location = sys.argv[4]

	sparkconf = SparkConf()
	sparkconf.setAppName("Build bipartite graphs")
	spark = SparkContext(conf=sparkconf)
	spark.setLogLevel('ERROR')
	sqlContext = SQLContext(spark)

	triples = spark.textFile(microdata_triples_location)

	companies = spark.textFile(company_triples_location)

	# split the parts into triples (not perfect)
	parts = triples.map(split_into_triples).filter(lambda a: a is not None)
	parts_comp = companies.map(split_into_triples).filter(lambda a: a is not None)

	###############################
	# PRODUCT MATHING IS DONE HERE

	# remove @et and @et-ee language strings
	parts = parts.map(remove_language_strings)
	# remove SKUs that are null or na
	parts = parts.filter(remove_empty_skus)

	# find actual duplicates
	# replace node_ids with sku's
	connections = parts.map(add_sku_relationship).filter(lambda a: a is not None)

	# END PRODUCT MATCHING
	###############################

	# add companies and products
	parts = parts.union(parts_comp)
	parts = parts.union(connections)

	values_rdd = parts.map(lambda p: Row(id=p[0], predicate=p[1], object=p[2]))
	values = sqlContext.createDataFrame(values_rdd)
	values.cache()

	# todo refactor column names
	# todo performance of these joins could be improved for sure
	companies = values\
		.filter(values.predicate == '<http://graph.ir.ee/media/usedBy>')\
		.select(F.col("id").alias("company_id"), F.col("object").alias("company_nr"))

	items = values\
		.filter(values.predicate == '<http://www.w3.org/1999/xhtml/microdata#item>')\
		.select(F.col("id").alias("item_id"), F.col("object").alias("item_ref"))

	products = values\
		.filter(values.object == '<http://schema.org/Product>')\
		.select(F.col("id").alias("product"))

	# todo what happens if we already have a same as connection?
	connections = values\
		.filter(values.predicate == '<http://schema.org/Product/sameAs>')\
		.select(F.col("id").alias("item_id"), F.col("object").alias("connection_ref"))

	# connect products and sameAs references
	product_connections = products\
		.join(connections, products.product == connections.item_id, 'left')\
		.select("product", "connection_ref")

	company_items = companies.join(items, companies.company_id == items.item_id, 'left').distinct()
	company_items = company_items.filter(company_items.item_ref != "null")

	company_products = company_items.join(product_connections, company_items.item_ref == products.product, 'left')
	company_products = company_products.filter(company_products.product != "null")

	# write all connections do a separate file
	company_products\
		.filter(company_products.connection_ref != "null")\
		.select("company_nr", F.col("connection_ref").alias("product"))\
		.write.parquet(output_filtered_location)

	# todo can this be done using dataframe operations?
	# write only matched products
	company_products.registerTempTable("company_products_table")
	sqlContext.sql(
		"select company_nr, IF(connection_ref != \"null\", connection_ref, product) as product from company_products_table")\
		.write.parquet(output_all_location)

	spark.stop()
