# pySpark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("DataSkewExample").getOrCreate()
