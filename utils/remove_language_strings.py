# filter skus
def remove_language_strings(sku):
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

out = open("cleaned.csv", "w")

for line in contents:
	lst = eval(line)
	sku = remove_language_strings(lst[-1])
	out.write(",".join([str(lst[0]), lst[1], lst[2], sku]) + "\n")

out.close()