import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql import Row
spark=SparkSession.builder.config(conf=SparkConf()).getOrCreate()

from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline,PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vector,Vectors
from pyspark.sql import Row
from pyspark.ml.feature import IndexToString,StringIndexer,VectorIndexer

workclass = {}
education = {}
marital_status = {}
occupation = {}
relationship = {}
race = {}
sex = {}
native_country = {}

def data_deal(v):#数据处理
    if v[1] not in workclass:
        workclass[v[1]]=len(workclass)
    if v[3] not in education:
        education[v[3]]=len(education)
    if v[5] not in marital_status:
        marital_status[v[5]]=len(marital_status)
    if v[6] not in occupation:
        occupation[v[6]] = len(occupation)
    if v[7] not in relationship:
        relationship[v[7]] = len(relationship)
    if v[8] not in race:
        race[v[8]] = len(race)
    if v[9] not in sex:
        sex[v[9]] = len(sex)
    if v[13] not in native_country:
        native_country[v[13]] = len(native_country)
    feature_str=[workclass[v[1]],education[v[3]],marital_status[v[5]],\
                    occupation[v[6]],relationship[v[7]],race[v[8]],\
                    sex[v[9]],native_country[v[13]]]
    return feature_str

def f(x):
    feature_str=data_deal(x)#字符数据数值化
    rel={}
    rel['features'] = Vectors.dense(float(x[0]), float(feature_str[0]),
                                    float(x[2]),float(feature_str[1]),
                                    float(x[4]), float(feature_str[2]),
                                    float(feature_str[3]), float(feature_str[4]),
                                    float(feature_str[5]), float(feature_str[6]),
                                    float(x[10]), float(x[11]),
                                    float(x[12]), float(feature_str[7]))
    rel['label']=str(x[14])
    return rel
    



#读取训练集和测试集，生成DataFrame
training_data=spark.sparkContext.textFile('D:\\本科\\时空数据处理与组织\\实习\\data\\adult_train.txt').\
        filter(lambda line:(len(line.strip()) > 0) and (len(line.split(", "))== 15)).\
        map(lambda line:line.split(", ")).\
        map(lambda line :Row(**f(line))).\
        toDF()
test_data=spark.sparkContext.textFile('D:\\本科\\时空数据处理与组织\\实习\\data\\adult_test.txt').\
        filter(lambda line:(len(line.strip()) > 0) and (len(line.split(", "))== 15)).\
        map(lambda line:line.strip(".")).\
        map(lambda line:line.split(", ")).\
        map(lambda line :Row(**f(line))).\
        toDF()

#处理特征和标签
labelIndexer=StringIndexer().setInputCol('label').\
        setOutputCol('indexedLabel').fit(training_data)
featureIndexer=VectorIndexer().setInputCol('features').\
        setOutputCol('indexedFeatures').setMaxCategories(14).\
        fit(training_data)
labelConverter=IndexToString().setInputCol('prediction').\
        setOutputCol('predictedLabel').setLabels(labelIndexer.labels)

#创建决策树模型
dtClassifier=DecisionTreeClassifier().\
            setLabelCol('indexedLabel').\
            setFeaturesCol('indexedFeatures')

#构建机器学习流水线
dtPipeline=Pipeline().\
        setStages([labelIndexer,featureIndexer,dtClassifier,labelConverter])
dtPipelineModel=dtPipeline.fit(training_data)#训练
dtPredictions=dtPipelineModel.transform(test_data)#测试
dtPredictions.select('predictedLabel','label','features').show(20)

#精度评定
evaluator=MulticlassClassificationEvaluator().\
    setLabelCol('indexedLabel').\
    setPredictionCol('prediction')
dtAccuracy=evaluator.evaluate(dtPredictions)
print('模型预测准确率为:'+str(dtAccuracy))
        
