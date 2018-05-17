from pyspark import SparkContext


sc = SparkContext()

# select random triple
for i in range(50):
  companyfile = sc.textFile('thesis/outputs/company-triples3/' + "part-" + "%05d" % (random.randint(0, 10520),)).collect()
  print random.choice(companyfile)


# select random complex triple
for i in range(50):
  companyfile = sc.textFile('thesis/outputs/company-triples3/' + "part-" + "%05d" % (random.randint(0, 10520),)).collect()
  #a = random.choice(companyfile)
  counter = 0
  while counter < 10000:
    a = random.choice(companyfile)
    if len(a.split(">")[0].split("//")[1].split("/")[0].replace("www.", "").split(".")) != 2:
      print a
      break
    counter += 1

# select random triple that is not connected to a company
for i in range(50):
  companyfile = sc.textFile('thesis/outputs/company-triples-code-or-domain2/' + "part-" + "%05d" % (random.randint(0, 10520),)).collect()
  #a = random.choice(companyfile)
  counter = 0
  while counter < 10000:
    b = random.choice(companyfile)
    if not "graph.ir.ee" in b.split("<")[3]:
      print b
      break
    counter += 1
