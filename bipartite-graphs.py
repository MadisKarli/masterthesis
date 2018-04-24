from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

import os
from pyspark.sql import functions as F
#import graphframes
from pyspark.sql.types import BooleanType

from pyspark.sql.functions import udf
import sys
import re
import csv


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
	row = row.encode('utf-8')
	res = [x for x in csv.reader([row], delimiter=' ')][0]
	if len(res) == 4:
		if res[3] == ".":
			return res


if __name__ == "__main__":
	# this works from command line
	# spark-submit  --packages graphframes:graphframes:0.5.0-spark1.6-s_2.10 bipartite-graphs.py
	# os.environ['PYSPARK_SUBMIT_ARGS'] = '  --packages graphframes:graphframes:0.5.0-spark1.6-s_2.10  pyspark-shell '

	spark = SparkContext(appName="PythonDataFrame")

	spark.setLogLevel('ERROR')

	sqlContext = SQLContext(spark)

	# filterEdges_udf = udf(filterEdges, BooleanType())
	# filterCompanies_udf = udf(filterCompanies, BooleanType())
	# filterProducts_udf = udf(filterProducts, BooleanType())


	lines = spark.textFile("/home/madis/IR/jupyter notebooks/merged-tpiletee")
	#lines = spark.textFile("test/example.nt")
	parts = lines.map(split_into_triples).filter(lambda a: a is not None)

	values_rdd = parts.map(lambda p: Row(id=p[0], predicate=p[1], object=p[2]))
	values = sqlContext.createDataFrame(values_rdd)

	values.registerTempTable("triples")

	# todo convert to dataframe operations
	companies = sqlContext.sql("SELECT id as company_id, object as company_nr FROM triples t WHERE predicate = '<http://graph.ir.ee/media/usedBy>' ")
	items = sqlContext.sql("SELECT id as item_id, object as item_ref FROM triples t WHERE predicate = '<http://www.w3.org/1999/xhtml/microdata#item>'")
	products = sqlContext.sql("SELECT id as product FROM triples t WHERE object = '<http://schema.org/Product>'")

	# todo now that we have triples we can find duplicate products
	# find all products
	# values.filter(F.col("predicate").rlike("<http://schema.org/Product/")).show(10, False)
	# find all products with SKU

	company_items = companies.join(items, companies.company_id == items.item_id, 'left').distinct()
	company_items = company_items.filter(company_items.item_ref != "null")

	company_products = company_items.join(products, company_items.item_ref == products.product, 'left')
	company_products = company_products.filter(company_products.product != "null")

	bipartite = company_products.select("company_nr", "product")

	rdd = bipartite.write.parquet("bipartite")

	spark.stop()
