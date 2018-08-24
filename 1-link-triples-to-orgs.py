import pyspark
from pyspark.sql.types import Row
import sys
import time


def populate_companies(file_loc):
	db = {}
	# todo also clean the companies
	# such as fb.com and facebook.ee should be facebook.com
	# lowercase the names, there were WWWs in the list
	with open(file_loc, 'r') as f:
		# skip the header
		f.readline()
		contents = f.read().split("\n")
		for row in contents:
			parts = row.split(";")
			if len(parts) == 2:
				db[parts[1].strip("\r").replace("http://", "").replace("https://", "")] = parts[0]
	return db


def split_row(row):
	x = row
	row = row[:-2].split(" ", 2)
	domain = None if (x.startswith("_:node") or x.startswith(":node")) else x.replace("http://", "").replace("https://", "").replace("www.", "").split("> ")[0][1:].split("/")[0]
	return Row(DOMAIN=domain, s=row[0], p=row[1], o=row[2])


def insert_company_triples(row):
	# facebook.com, fb.com, facebook.ee
	# sites.google.com
	# hot.ee
	# etsy.com
	# vk.com
	# instagram.com
	# youtube.com
	# spotlight.com
	# theharterallenagency.com
	# hot.ee
	# infonet.ee
	# tallinn.ee ?
	# eelk.ee?
	# web.zone.ee
	# zone.ee
	# osta.ee
	# ut.ee
	# ttu.ee
	row = row.encode('utf-8')
	mined_from_url_clean = row.split("> ")[0][1:]
	mined_from_url = mined_from_url_clean.replace("http://", "").replace("https://", "")

	domain1 = ".".join(mined_from_url.replace("www.", "").split("/")[0].split(".")[-2:])

	if mined_from_url.startswith(":node"):
		return None

	# match with database information
	# we do it ths way as we also need partial matches
	# if we go with full matches (lose some information), then there are faster ways
	for j in company_database:
		if j in mined_from_url.lower():
			# check if domains match, from server.domain.com/path we only use domain.com
			domain2 = ".".join(j.replace("www.", "").split("/")[0].split(".")[-2:])

			if domain1 == domain2:
				if cluster:
					company_id = company_database[j][0]
				else:
					company_id = company_database[j]
				# return company_id + " " + j
				return "<" + mined_from_url_clean + "> " \
													"<http://graph.ir.ee/media/usedBy> " \
													"<https://graph.ir.ee/organizations/ee-" + company_id + "> ."

	# if we don't have company code then return the domain only
	return "<" + mined_from_url_clean + "> <http://graph.ir.ee/media/usedBy> <" + domain1 + "> ."


if __name__ == "__main__":
	if len(sys.argv) < 4:
		print("Usage:")
		print("link-triples-to-orgs.py company_codes_csv_location input_triples_loc output_loc")
		print("\nExample:")
		print("python link-triples-to-orgs.py "
			  "/home/madis/IR/data/company-urls/company-urls.csv "
			  "/home/madis/IR/data/microdata_from_warcs/skumatch "
			  "/tmp/triples-to-companies")
		sys.exit(1)
	else:
		company_csv_list_loc = sys.argv[1]
		input_loc = sys.argv[2]
		output_loc = sys.argv[3]

	start = time.time()

	cluster = False

	s_conf = pyspark.SparkConf()
	s_conf.set("spark.executor.instances", "60")
	s_conf.set("spark.dynamicAllocation.enabled", "false")

	sc = pyspark.SparkContext(conf=s_conf)
	sc.setLogLevel("ERROR")
	sqlContext = pyspark.SQLContext(sc)

	if cluster:
		array = ["Not Disclosed - Visit www.internet.ee for webbased WHOIS"]

		company_data = sqlContext.read.format("org.apache.phoenix.spark").option("table", 'IR.STG_DOMAIN').option("fetchSize",
																											"10000").option(
			"numPartitions", "5000").option("zkUrl", "ir-hadoop1,ir-hadoop2,ir-hadoop3:2181").load()

		company_df = company_data.select(company_data.DOMAIN, company_data.REG_CODE).filter(
			company_data.REG_CODE.isin(*array) == False).distinct()

		company_database = company_df.toPandas().set_index('DOMAIN').T.to_dict('list')
	else:
		company_database = populate_companies(company_csv_list_loc)

	reader = sc.textFile(input_loc)

	with_companies = reader\
		.map(insert_company_triples)\
		.filter(lambda a: a is not None)\
		.distinct()

	with_companies.saveAsTextFile(output_loc)

	sc.stop()

	print("time taken: " + str(time.time() - start))
	print("input: " + input_loc)
	print("-----------------------------------------------------------------------")
