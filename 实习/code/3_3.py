import findspark
findspark.init()

from pyspark.sql.types import *
from pyspark.sql import Row,SparkSession
from pyspark import SparkContext,SparkConf

spark = SparkSession.builder.config(conf = SparkConf()).getOrCreate()


#设置要插入的数据
employee_rdd=spark.sparkContext.parallelize(['3,Mary,F,26','4,Tom,M,23'])\
                .map(lambda line:line.split(","))

#创建Row数据
employee_row=employee_rdd.map(lambda line:Row(id=int(line[0]),name=line[1],gender=line[2],age=int(line[3])))

#把数据和模式对应起来
employee_DF=spark.createDataFrame(employee_row)

#把数据写入数据库
prop = {}
prop['user'] = 'root'
prop['password'] = '090012'
prop['driver'] = "com.mysql.jdbc.Driver"
employee_DF.write.jdbc("jdbc:mysql://localhost:3306/testspark",'employee','append', prop)