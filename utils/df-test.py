from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as functions
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkContext()
spark.setLogLevel("ERROR")
sql_context = SQLContext(spark)

data = spark.parallelize([
	("company1", 2, 2.0),
	("company2", 2, 4.0),
	("company3", 1, 1.0),
	("company4", 1, 0.0),
	("company5", 1, 2.0),
])

schema = StructType([
	StructField("id", StringType(), True),
	StructField("degree", IntegerType(), True),
	StructField("nnd", FloatType(), True)
])


df = sql_context.createDataFrame(data, schema)
df.show()

# average nearest neighbour degree from nearest neighbour degree
annd = df\
	.join(
	df.groupBy(df.degree).avg("nnd"), "degree")\
	.select(F.col("id"), F.col("nnd"), F.col("avg(nnd)").alias("test"))

annd.sort(F.asc("id")).show()
