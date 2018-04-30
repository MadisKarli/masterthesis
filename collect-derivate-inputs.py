import os
import pandas as pd
import numpy as np

directory_name = "metrics"
# todo what if we have a company that is not represented at all?
arr = os.listdir(directory_name)
arr.sort()

# read the first file and add 00 to its column names so that we have degree00, pagerank00, ...
dir_df = pd.read_csv(directory_name + "/" + arr[0], index_col="ID")
dir_df.columns = [x + "00" for x in dir_df.columns]

# read csv files and add 01, 02,... to column names based on the index of file
for idx, f in enumerate(arr[1:]):
	df = pd.read_csv(directory_name + "/" + f, index_col="ID")
	# adds the 01, 00, etc to column names
	df.columns = [x + str('{:02d}'.format(idx + 1)) for x in df.columns]
	dir_df = pd.concat([dir_df, df], axis=1)

# generate values if we don't have 49 columns
# todo what should we do when new metric is added halfway through, create 0s before the time?

time_series_variables = ['degree', 'pagerank']
# calculate how many extra variables we need to generate
variable_count = dir_df.shape[1] / len(time_series_variables)
needed = 49 - variable_count

# generate an output file for each column name as derivatives does not support multiple time series
row_names = dir_df.index

# if we want to generate additional data
generate_data_bool = True

for ts in time_series_variables:
	ts_only = dir_df.filter(like=ts)
	if generate_data_bool:
		colnames = [ts + str('{:02d}'.format(variable_count + 1 + x)) for x in range(needed)]
		generated_values = np.random.rand(len(row_names), len(colnames))
		generated_df = pd.DataFrame(columns=colnames, index=row_names, data=generated_values)
		combined_df = pd.concat([ts_only, generated_df], axis=1)
		combined_df.to_csv(ts + ".csv", na_rep=0)
		print combined_df
	else:
		combined_df.to_csv(ts + ".csv", na_rep=0)
