import pyspark


def populate_companies(file_loc):
	db = {}
	with open(file_loc, 'r') as f:
		header = f.readline()
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


def contains_company_url(row):
	row = row.encode('utf-8')
	if row[0] == "<":
		mined_from_url = row.split("> ")[0][1:]
		# match with database information
		for j in company_database:
			if j in mined_from_url:
				return True
	return False


def insert_company_triples(row, wantShort=True):
	row = row.encode('utf-8')
	mined_from_url = row.split("> ")[0][1:]

	# match with database information
	for j in company_database:
		if j in mined_from_url:
			company_id = company_database[j]

			if wantShort:
				# Peep's idea
				return "<" + mined_from_url + "> <http://graph.ir.ee/media/usedBy> <https://graph.ir.ee/organizations/ee-" + company_id + "> ."

			# Or we could create a node for each company
			return create_estonian_company_triples(mined_from_url, company_id)


company_database = populate_companies('company-urls.csv')

configuration = pyspark.SparkConf()
configuration.set("spark.executor.cores", "3")

sc = pyspark.SparkContext(conf=configuration)

reader = sc.textFile("hdfs://ir-hadoop2/user/madis/microdata/2017/triples-microdata")

# distinct 16:45:01 - 16:46:16
# no distinct 16:48:46 - 16:50:49
with_companies = reader.filter(contains_company_url).distinct().map(insert_company_triples)
with_companies.coalesce(1).saveAsTextFile("thesis/companiy-triples")


# join companies and triples
#combined = reader.union(with_companies)
#combined.saveAsTextFile("tmp/merged-tpiletee01")

sc.stop()
