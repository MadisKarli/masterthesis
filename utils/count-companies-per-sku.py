import operator

# filter skus
def remove_language_strings(sku):
	if "EHA6041XOK" in sku:
		print sku
	parts = sku.split("@")
	language_tag = parts[-1]
	# case 1: @et or @fi at the end
	if len(language_tag) == 2:
		sku = "@".join(parts[:-1])
	# case 2: @et-ee at the end
	elif len(language_tag) == 5:
		if language_tag[2] == "-":
			sku = "@".join(parts[:-1])
	return sku


contents = open("/home/madis/IR/data/microdata_from_warcs/company-to-sku.csv").read().split("\n")

contents = [x for x in contents if len(x) > 5]

db = {}

for line in contents:
	lst = eval(line)
	sku = remove_language_strings(lst[-1]).strip('\"')
	company = lst[0]
	if company is None:
		continue
	if sku == "-" or sku == "":
		continue
	if sku in db:
		asi = db[sku] + "|" + company
		db[sku] = asi
	else:
		db[sku] = company

cnt = 0
max_cnt = 0
max_sku = ""

counts = {}
#out = open("sku-companies-counts.tsv", "w")
for i in db:
	comps = set(db[i].split("|"))
	comps_str = ",".join(comps)
	comps_cnt = len(comps)
	if comps_cnt > 1:
		counts[i] = comps_cnt
		# print i, comps_str
		cnt = cnt + 1

	#out.write(i + '\t' + comps_str + '\n')


#out.close()
out = open("sku_company_counts.csv", "w")
sorted_x = sorted(counts.items(), key=operator.itemgetter(1))
for x in sorted_x:
	out.write(x[0] + "," + str(x[1]) + "\n")
# 70003785
out.close()
