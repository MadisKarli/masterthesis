#before running
#alias python=python3 
# Exception: Python in worker has different version 2.7 than that in driver 3.5, PySpark cannot run with different minor versions


# install all required r libraries
install.packages(c("psych", "gtools", "dplyr"))
devtools::install_github("robjhyndman/anomalous-acm")

# set spark to run python3
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
