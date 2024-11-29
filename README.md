# pySpark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("DataSkewExample").getOrCreate()

# Sample Dataset
data = [
    (1, "US-East", "A", 3, 20.0),
    (2, "US-West", "B", 2, 50.0),
    (3, "US-East", "C", 5, 30.0),
    (4, "EU-Central", "A", 1, 25.0),
    (5, "US-East", "B", 10, 15.0)
]

columns = ["OrderID", "Region", "ProductID", "Quantity", "Price"]
orders_df = spark.createDataFrame(data, columns)

# Calculate Revenue
orders_df = orders_df.withColumn("Revenue", col("Quantity") * col("Price"))

# Group by Region
result = orders_df.groupBy("Region").agg(sum("Revenue").alias("TotalRevenue"))
result.show()
