from pyspark import SparkContext, SparkConf

#SparkContext.setSystemProperty('spark.executor.memory', '12g')
#sc = SparkContext(appName="skumapper")

company_database = {}

#with open("/home/madis/IR/data/company-urls/company-urls.csv", 'r') as f:
with open("thesis/company-urls.csv", 'r') as f:
    header = f.readline()
    contents = f.read().split("\n")
    for row in contents:
        parts = row.split(";")
        if len(parts) == 2:
            company_database[parts[1].strip("\r").replace("http://", "").replace("https://", "")] = parts[0]

def find_company_code(url):
    mined_from_url = url
    # match with database information
    for j in company_database:
        if j in mined_from_url:
            company_id = company_database[j]
            return company_database[j]
        
def split_to_triples(row):
    import csv
    csv.field_size_limit(100000000)
    row = row.encode('utf-8')
    parts = [x for x in csv.reader([str(row)], delimiter=' ')][0]
    if len(parts) == 4:
        #if parts[0].startswith("_:node"):
        return parts

        
db = sc.broadcast(company_database)
skus = open("thesis/microdata2017_mined2018-skus-usable.txt").read()
#skus = open("/home/madis/IR/data/microdata_from_warcs/microdata2017_mined2018-skus-usable.txt").read()
#(node_id, sku)
tuples = []
# print len(skus.split("\n"))
for i in skus.split("\n"):
    try:
        triple =  "_:node" + i.split("_:node")[1]
        triple = triple.split(" ")
        node_id = triple[0]
        sku = triple[-2]
        tuples.append((node_id, sku))
    except IndexError:
        triple = "https://" + i.split("<https://")[1]
        triple = triple.split(" ")
        node_id = triple[0]
        sku = triple[-2]
        tuples.append((node_id, sku))
        
tuples_bc = sc.broadcast(tuples)

def connect_to_company(row):
    #row = [object, predicate, subject]
    if row[0].startswith("<http"):
        if row[2].startswith("_:node"):
            for t in tuples_bc.value:
                if t[0] == row[2]:
                    company = find_company_code(row[0])
                    return [company, row[0], t[0], t[1]]
    
        
#"/home/madis/IR/data/microdata_from_warcs/skumatch"
reader = sc.textFile("hdfs://ir-hadoop1/user/madis/microdata/2017/triples-microdata").map(split_to_triples).filter(lambda a: a is not None)
comp = reader.map(connect_to_company).filter(lambda a: a is not None)

for i in comp.take(10):
    print i
