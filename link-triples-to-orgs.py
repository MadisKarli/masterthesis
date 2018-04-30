import pyspark
import sys


def populate_companies(file_loc):
	db = {}
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

	reader = sc.textFile(input_loc)

	with_companies = reader.filter(contains_company_url).distinct().map(insert_company_triples)
	with_companies.coalesce(1).saveAsTextFile(output_loc)

	# join companies and triples
	# combined = reader.union(with_companies)
	# combined.saveAsTextFile("tmp/merged-tpiletee01")

	sc.stop()
