import os
import pandas as pd
import numpy as np
import csv

dir = "metrics"
# todo what if we have a company that is not represented at all?
arr = os.listdir(dir)
arr.sort()

dir_df = pd.read_csv(dir + "/" + arr[0], index_col="ID")
dir_df.columns = [x + "00" for x in dir_df.columns]

for idx, f in enumerate(arr[1:]):
	df = pd.read_csv(dir + "/" + f, index_col="ID")
	# adds the 01, 00, etc to column names
	df.columns = [x + str('{:02d}'.format(idx + 1)) for x in df.columns]
	dir_df = pd.concat([dir_df, df], axis=1)
	# renames the columns but not all
	# dir_df = pd.merge(dir_df, df, left_index=True, right_index=True, how='outer')


print dir_df

# todo change if new metrics are added
# generate values if we dont have 49 columns
# divided by two as we have degree and pagerank
degree_count = dir_df.shape[1] / 2
needed = 49 - degree_count

colnames1 = ['degree' + str('{:02d}'.format(degree_count + 1 + x)) for x in range(needed)]
colnames2 = ['pagerank' + str('{:02d}'.format(degree_count + 1 + x)) for x in range(needed)]
colnames = colnames1 + colnames2

rownames = dir_df.index
generated_data = np.random.rand(len(rownames), len(colnames))

generated_df = pd.DataFrame(columns=colnames, index=rownames, data=generated_data)
generated_data = pd.concat([generated_df, dir_df], axis=1)

generated_data = generated_data.reindex(sorted(generated_data.columns), axis=1)

generated_data.to_csv("asi.csv", na_rep=0)
