import pyspark
import sys


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


def create_estonian_company_triples(mined_from, company_code):
	company_node_id = "_:company" + company_code

	a = "<" + mined_from + "> <http://www.w3.org/1999/xhtml/microdata#media> " + company_node_id + " .\n"
	b = company_node_id + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://graph.ir.ee/media> .\n"
	c = company_node_id + " <http://graph.ir.ee/media/usedBy> \"" + company_code + "\" ."

	return a + b + c


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

	if mined_from_url.startswith(":node"):
		return None

	# match with database information

	# 40 matches when only checking domain
	# 53 when checking for longest match, but a lot of false positives?
	for j in company_database:
		if j in mined_from_url.lower():
			# check if domains match, from server.domain.com/path we only use domain.com
			domain1 = ".".join(mined_from_url.replace("www.", "").split("/")[0].split(".")[-2:])
			domain2 = ".".join(j.replace("www.", "").split("/")[0].split(".")[-2:])

			if domain1 == domain2:
				company_id = company_database[j]
				# return company_id + " " + j
				return "<" + mined_from_url_clean + "> " \
													"<http://graph.ir.ee/media/usedBy> " \
													"<https://graph.ir.ee/organizations/ee-" + company_id + "> ."


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

	company_database = populate_companies(company_csv_list_loc)

	sc = pyspark.SparkContext()
	# sc.setLogLevel("ERROR")

	reader = sc.textFile(input_loc)

	with_companies = reader\
		.map(insert_company_triples)\
		.filter(lambda a: a is not None)\
		.distinct()

	with_companies.saveAsTextFile(output_loc)

	# join companies and triples
	# combined = reader.union(with_companies)
	# combined.saveAsTextFile("tmp/merged-tpiletee01")

	sc.stop()
