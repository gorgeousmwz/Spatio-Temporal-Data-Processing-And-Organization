import findspark
findspark.init()

from pyspark.sql.types import *
from pyspark.sql import Row,SparkSession
from pyspark import SparkContext,SparkConf

spark = SparkSession.builder.config(conf = SparkConf()).getOrCreate()

#连接数据库
jdbcDF =spark.read.format("jdbc").option("driver","com.mysql.jdbc.Driver")\
    .option("url", "jdbc:mysql://localhost:3306/testspark").option("dbtable", "employee")\
    .option("user", "root").option("password", "090012").load()
jdbcDF.show()

#最大值
max=jdbcDF.describe('age').filter("summary='max'")\
        .select('age').collect()[0].asDict()['age']
print('The max of age is '+max)

#总和
mean=jdbcDF.describe('age').filter("summary='mean'")\
        .select('age').collect()[0].asDict()['age']
count=jdbcDF.describe('age').filter("summary='count'")\
        .select('age').collect()[0].asDict()['age']
sum=float(mean)*float(count)
print('The sum of age is '+str(sum))

