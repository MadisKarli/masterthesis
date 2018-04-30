#before running
#alias python=python3 
# Exception: Python in worker has different version 2.7 than that in driver 3.5, PySpark cannot run with different minor versions


# install all required r libraries
install.packages(c("psych", "gtools", "dplyr"))
devtools::install_github("robjhyndman/anomalous-acm")

# set spark to run python3
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

#Caused by: java.lang.IllegalStateException: Input row doesn't have expected number of values required by the schema. 50 fields are required while 26 values are provided.

Fix: This means that there are unknown columns in the dataframe, delete columns that are not used in TSvars
