from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

import csv
import datetime
import sys


# expected output
# header row
# "ID","X01","X02","X03","X04","X05","X06"
# values rows
# 1, 1,0.749764988710541,2.94344455011988,1.78395274019694,1.03023966502446,0.918607517295546,0.749478356232481
# 2,-1.03990715504114,-2.12893943162453,-2.62987978192414,-2.77344403779796,-3.046014665944,-3.81802623541034
def row_to_str(row):
	out = []
	ir_str = "<https://graph.ir.ee/organizations/ee-"

	for i in row:
		tmp = i
		# get only company id
		if isinstance(i, unicode):
			if i.startswith(ir_str):
				tmp = i.replace(ir_str, "").replace(">", "")

		out.append(tmp)
	return out


if __name__ == "__main__":
	if len(sys.argv) < 3:
		print("Usage:")
		print("network-metrics-to-derivative-input.py location_of_bipartite_data output_location(existing folder)")
		print("\nExample")
		print("python network-metrics-to-derivative-input.py network-metrics metrics")
		sys.exit(1)
	else:
		bipartite_location = sys.argv[1]
		output_location = sys.argv[2]

	sc = SparkContext()
	sc.setLogLevel("ERROR")

	sqlContext = SQLContext(sc)

	values = sqlContext.read.load(bipartite_location, format="parquet")

	row_names = values.schema.names
	row_names[0] = "ID"

	company_metrics = values.filter(F.col("id").startswith("<https://graph.ir.ee"))

	output_contents = company_metrics.map(row_to_str).collect()

	# get date
	now = datetime.datetime.now()
	print "Current date" + str(now.strftime("%Y%m%d"))

	with open(output_location + '/' + str(now.strftime("%Y%m%d")) + '.csv', 'w') as csvfile:
		csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC)
		csvwriter.writerow(row_names)
		csvwriter.writerows(output_contents)
