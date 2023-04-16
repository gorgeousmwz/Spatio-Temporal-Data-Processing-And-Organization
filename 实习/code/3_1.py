import findspark
findspark.init()

from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

#创建DataFram
spark=SparkSession.builder.config(conf=SparkConf()).getOrCreate()
data=spark.read.json('D:\\本科\\时空数据处理与组织\\实习\\data\\employee.json')

#Promble 1
print('-----------------------------------------------------')
print('Promble 1:Show all data')
data.select('*').show()

#Promble 2
print('-----------------------------------------------------')
print('Promble 2:Deduplication')
data1=data.dropDuplicates()
data1.show()

#Promble 3
print('-----------------------------------------------------')
print('Promble 3:show and remove id')
data1.select('name','age').show()

#Promble 4
print('-----------------------------------------------------')
print('Promble 4:show data whose age >30')
data1.where('age>30').show()

#Promble 5
print('-----------------------------------------------------')
print('Promble 5:divide by age')
data1.groupBy('age').count().show()

#Promble 6
print('-----------------------------------------------------')
print('Promble 6:sort name in ascend')
data1.orderBy(data['name'].asc()).show()

#Promble 7
print('-----------------------------------------------------')
print('Promble 7:show top 3 rows')
data1.show(3)

#Promble 8
print('-----------------------------------------------------')
print('Promble 8:select name as username')
data1.selectExpr('name as username').show()

#Promble 9
print('-----------------------------------------------------')
print('Promble 9:select age\'s mean')
mean=data1.describe('age').filter("summary='mean'")\
    .select('age').collect()[0].asDict()['age']
print('The mean of age is '+mean)

#Promble 10
print('-----------------------------------------------------')
print('Promble 10:select age\'s min')
min=data1.describe('age').filter("summary='min'")\
    .select('age').collect()[0].asDict()['age']
print('The mean of age is '+min)