import findspark
findspark.init()

from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark=SparkSession.builder.config(conf=SparkConf()).getOrCreate()

def print_result(result):
    print()
    v=result.asDict()
    print('id:'+v['id']+',name:'+v['name']+',age:'+v['age'])


#创建rdd对象
data_rdd=spark.sparkContext.textFile('D:\\本科\\时空数据处理与组织\\实习\\data\\employee.txt')
#格式转换
data_rdd=data_rdd.map(lambda line:line.split(",")).\
        map(lambda x:Row(id=x[0],name=x[1],age=x[2]))
#转换为DataFrame
data=spark.createDataFrame(data_rdd)
#输出
data.foreach(print_result)
