#coding=utf-8
#初始化找到本机spark环境    
import findspark
findspark.init()

from pyspark import SparkConf,SparkContext

sc=SparkContext('local','test2')#创建SparkContext对象
data=sc.textFile('D:\\本科\\时空数据处理与组织\\实习\\data\\实习2啤酒销量数据.txt')#加载数据
#异常数据剔除(去头文件；去逗号；去引号；去制表符；去矛盾数据)
print('-----------------------------------------------------')
print('Promble 1:数据预处理')
head=data.first()#记录表头(这里不能使用take(1))
data=data.filter(lambda line:line!=head).\
    map(lambda x:x.replace(",","")).\
    map(lambda x:x.replace("\"","")).\
    map(lambda line:line.split('\t')).\
    filter(lambda line:int(line[3])*3-int(line[4])-int(line[5])-int(line[6])<3)

#去除整月销量为0的数据
print('-----------------------------------------------------')
print('Promble 2:月销量为0的数据剔除')
data=data.filter(lambda line:int(line[3])!=0)
print('Data culling is completed\nThe number of remaining data is '+str(data.count()))

#统计啤酒类型
print('-----------------------------------------------------')
print('Promble 3:啤酒类型统计')
result1=data.map(lambda line:(line[1],1)).\
        reduceByKey(lambda a,b:a+b)
print("The number of Beer-Type is "+str(result1.count()))

#销量最好的5种啤酒
print('-----------------------------------------------------')
print('Promble 4:销量最好的5种啤酒')
result2=data.map(lambda line:(line[1],int(line[4])+int(line[5])+int(line[6]))).\
        reduceByKey(lambda a,b:a+b).\
        sortBy(lambda x:x[1],False).take(5)
for i in range(5):
    print('The Number-'+str(i+1)+' is '+result2[i][0]+' (Sales:'+str(result2[i][1])+')')

#在去年销量大于500的区域中，哪个销售区域销售的啤酒同比去年增长最快
print('-----------------------------------------------------')
print('Promble 5:同比销量增长最快区域')
result3=data.map(lambda line:(line[2],((int(line[4])+int(line[5])+int(line[6])),(int(line[8])*0.75)))).\
        reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1])).\
        filter(lambda x:x[1][1]>=500).\
        sortBy(lambda x:x[1][0]/x[1][1],False).take(1)
print('The regions with the fastest year-on-year growth is '+result3[0][0])
print('Growth Rate is '+str(result3[0][1][0]/result3[0][1][1]-1))

#统计每种啤酒前三周的销量
print('-----------------------------------------------------')
print('Promble 6:统计每种啤酒前三周的销量')
result4=data.map(lambda line:(line[1],int(line[4])+int(line[5])+int(line[6]))).\
        reduceByKey(lambda a,b:a+b).\
        sortBy(lambda x:x[1],False).\
        collect()
for line in result4:
    print(line[0]+'\'s sales in the first three weeks of November is '+str(line[1]))
   
#统计啤酒卖得最好的前三个区域的11月份前3周销量
print('-----------------------------------------------------')
print('Promble 7:统计啤酒卖得最好的前三个区域的11月份前3周销量量')
result5=data.map(lambda line:(line[2],int(line[4])+int(line[5])+int(line[6]))).\
        reduceByKey(lambda a,b:a+b).\
        sortBy(lambda x:x[1],False).take(3)
for i in range(3):
    print('The Number-'+str(i+1)+' is '+result5[i][0]+'(Sales:'+str(result5[i][1])+')')
