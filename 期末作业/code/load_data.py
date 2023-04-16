import pymongo
from pymongo import MongoClient
import json

def load_data():
    #打开文件
    filename="D:\\gadm404-shp\\gadm404.json"
    file=open(filename,"r",encoding="utf-8")#以只读的模式打开json文件

    #连接mongdb,创建client
    client=MongoClient('localhost',27017)
    #连接数据库test
    db=client.test
    #指定集合mawenzhuo
    collection=db.mawenzhuo

    #逐行导入文件
    index=0#索引
    start=5#跳过前面的文件头
    for everyline in file:
        index=index+1
        if index<start:#跳过文件头
            continue
        elif index==2861 or index==36798 or index==40635:#跳过超过16M的数据
            continue
        else:
            if everyline[-2]==',':#中间行
                print(index)
                each_data=json.loads(everyline[:-2])#载入每一条数据
                #print(each_data)
                try:
                    collection.insert(each_data)#插入数据库集合
                except Exception as e:
                    print('ERRO'+str(index)+':')
                    print(e)
                print("--------------------------------------------------------------------")
            elif len(everyline)>4:#最后一行
                print('last line:',index)
                each_data=json.loads(everyline[:-1])
                #print(each_data)
                try:
                    collection.insert(each_data)#插入数据库集合
                except Exception as e:
                    print('ERRO'+str(index)+':')
                    print(e)
                print("--------------------------------------------------------------------")
            else:#读取完毕
                break
    
    #关闭文件
    file.close()



load_data();