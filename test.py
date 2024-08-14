from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("test_session").getOrCreate()


data = [
    ("Alice", 34),
    ("Bob", 45),
    ("Cathy", 29)
]

columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()

df.write.csv("test.csv", header=True, mode="overwrite")

spark.stop()