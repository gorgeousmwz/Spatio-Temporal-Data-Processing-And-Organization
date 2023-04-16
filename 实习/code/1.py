import findspark
findspark.init()

from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("FirstApp").getOrCreate()

data = spark.range(0, 10).select(col("id").cast("double"))

data.agg({'id': 'sum'}).show()

spark.stop()