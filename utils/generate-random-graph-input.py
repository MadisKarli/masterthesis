import numpy as np
import random
import string

# todo separate the companies and products

# 1 generate x random companies
# 2 generate y random products
# 3 generate z random urls for products
# 4 generate w random links between products and companies by connecting a random url to company and product
# in step 4 also create SKUs so that there would be matches


def random_alphanumeric(string_length):
	return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(string_length))


def random_lowercase(string_length):
	return ''.join(random.choice(string.ascii_lowercase) for _ in range(string_length))


# number of companies
x = 35000
# number of products
y = 15000000
output_comp = "graph-tests-millions/companies"
output_prod = "graph-tests-millions/products"
random.seed(123)

# step 1
# generate x random companies
companies = ["<https://graph.ir.ee/organizations/ee-" + str(i) + ">" for i in random.sample(range(100000), x)]

# generate y/2 sku values
skus = ["generated" + random_alphanumeric(10) for _ in xrange(int(y/2))]

# generate random distribution
# i % of companies represent 50 % of products
companies_size = len(companies)
top_companies_count = int(0.1 * companies_size)
top_companies_probs = top_companies_count * [(0.5 * companies_size/top_companies_count)/companies_size]
total_prob_top_companies = sum(top_companies_probs)
# rest of the companies represent other 50% of products
other_companies_probs = (companies_size - top_companies_count) * [(1 - total_prob_top_companies)/(companies_size - top_companies_count)]
probabilities = top_companies_probs + other_companies_probs
# remaining

# generate w random links between companies and products
print "generating"


# todo change to iterate over 10 000 elements at a time
for j in xrange(y//10000):
	print "iteration " + str(j)
	company_triples = []
	product_triples = []

	for _ in xrange(10000):
		product = "_:node" + random_alphanumeric(16)
		company = np.random.choice(companies, 1, p=probabilities)[0]
		sku = random.choice(skus)
		url = "<http://www." + random_lowercase(16) + ".ee>"

		product_url = url + " <http://www.w3.org/1999/xhtml/microdata#item> " + product + " ."
		product_type = product + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Product> ."
		product_sku = product + " <http://schema.org/Product/sku> " + sku + " ."
		company_url = url + " <http://graph.ir.ee/media/usedBy> " + company + " ."

		company_triples.append(company_url)
		product_triples.append("\n".join([product_url, product_type, product_sku]))

	output_file = open(output_comp + "part-" + str(i), "w")
	output_file.write("\n".join(company_triples))
	output_file.close()

	output_file = open(output_prod + "part-" + str(i), "w")
	output_file.write("\n".join(product_triples))
	output_file.close()

print "done"
