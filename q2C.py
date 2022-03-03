#Reference: http://files.grouplens.org/datasets/movielens/ml-25m-README.html
#The tag genome is a data structure that contains tag relevance scores for movies. The structure is a dense matrix: each movie in the genome has a value for every tag in the genome.
#genome-scores.csv - movieId (int) ,tagId (int),relevance (float)
#genome-tags.csv - tagId(int),tag (string)
#Reference: Jesse Vig, Shilad Sen, and John Riedl. 2012. The Tag Genome: Encoding Community Knowledge to Support Novel Interaction. ACM Trans. Interact. Intell. Syst.
from pyspark.sql.functions import *
import numpy as np
from numpy.linalg import norm 
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row, SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.param import Param
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.sql.functions import rand
from pyspark.ml.clustering import KMeans #K-means clustering with the k-means|| like initialization mode
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import col


spark = SparkSession.builder \
    .master("local[8]") \
    .appName("COM6012 Assignment 1 - Q2C") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")


df = spark.read.load("Data/ratings.csv",format = "csv", header = "true", delimiter=",")
rdd =df.rdd
#Before I can call ALS  , need to give each col the correct type, can do this using lambda functions
ratingsRDD = rdd.map(lambda p: Row(userId=int(float(p[0])), movieId=int(float(p[1])),rating=float(p[2]), timestamp=float(p[3])))
##Load other csvs for this question
genome_scores = spark.read.csv("Data/genome-scores.csv", inferSchema = "true",header = 'true')
#genome_tags = spark.read.csv("Data/genome-tags.csv", inferSchema = "true",header = 'true')

ratings = spark.createDataFrame(ratingsRDD)
ratings_split = ratings
#ratings_split = ratings.sample(False, 0.1) #sample the data for testing, comment this out after code is finished
#ratings_split.cache() #Reference: https://databricks.com/session/getting-the-best-performance-with-pyspark
#(training, test) = ratings_split.randomSplit([0.8, 0.2],seed = 1234)

### 3 different versions of ALS###

als = ALS(maxIter=10, regParam=0.1, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop", seed = 1234)
als2 = ALS(maxIter=20, regParam=0.1, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop", seed = 1234)
als3 = ALS(maxIter=10, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop", seed = 1234)

paramGrid = ParamGridBuilder().addGrid(als.regParam, [0.1, 0.01]).addGrid(als.maxIter, [10,20]).build()         
pipeline = Pipeline(stages=[als])
modelEvaluator=RegressionEvaluator(labelCol="rating",predictionCol="prediction") # by default this will use rmse
evaluator2 = RegressionEvaluator(metricName="mae", labelCol="rating",predictionCol="prediction")

#Crossvalidate
#I have switched back to Pysparks cross validator as reporting error per fold is not necessary here
crossval1 = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=modelEvaluator,
                          numFolds=3,seed = 1234)


#uncomment as necessary       
model = als.fit(ratings_split)
#cvModel1 = crossval1.fit(ratings_split)    
#model2 = als2.fit(training)  
#model3 = als3.fit(training)
dfItemFactors=model.itemFactors

#dfItemFactors.show()

# convert the data to dense vector in order to run kmeans algorithm
#def transData(data):
    #return data.rdd.map(lambda r: [Vectors.dense(r[1])]).toDF(['features'])
def transData(data):
    return data.rdd.map(lambda r: [int(r[0]),Vectors.dense(r[1])]).toDF(['id','features'])


dfFtrvec= transData(dfItemFactors) #id=movieid 
#dfFtrvec.cache()
#dfFtrvec.show(5, False)

k = 25
kmeans = KMeans().setK(k).setSeed(1234) #set k = 25s and set seed
model1 = kmeans.fit(dfFtrvec)
predictions = model1.transform(dfFtrvec)# Make predictions, i.e., get cluster index

#print("predictions")
predictions.show()

#Reference: https://towardsdatascience.com/k-means-implementation-in-python-and-spark-856e7eb5fe9b

summary = model1.summary
print(summary.clusterSizes)
arr = np.array(summary.clusterSizes)
temp = np.argpartition(-arr, 3)#need to get indices of largest 3 clusters
result = temp[:3]
print(result)

cluster1MoviesSplit1 = predictions.select('id').filter(predictions.prediction==int(result[0])).withColumnRenamed('id','movieId') #column needs to be renamed before a join
cluster2MoviesSplit1 = predictions.select('id').filter(predictions.prediction==int(result[1])).withColumnRenamed('id','movieId')
cluster3MoviesSplit1 = predictions.select('id').filter(predictions.prediction==int(result[2])).withColumnRenamed('id','movieId')

relGenScCl1Spl1 = genome_scores.join(cluster1MoviesSplit1,'movieId')
relGenScCl1Spl2 = genome_scores.join(cluster2MoviesSplit1,'movieId')
relGenScCl1Spl3 = genome_scores.join(cluster3MoviesSplit1,'movieId')

relGenScCl1Spl1.show()
Movies111=relGenScCl1Spl1.groupBy('tagId').sum() #sum up tag scores for all movies in it; 
#Movies111.show(5,False) # show top 3
Tags_sorted=Movies111.orderBy(desc('sum(relevance)')).show(3) #find the largest three scores and their indexes;

relGenScCl1Spl2.show()
Movies112=relGenScCl1Spl2.groupBy('tagId').sum() #sum up tag scores for all movies in it; 
#Movies112.show(5,False) # show top 3
Tags_sorted=Movies112.orderBy(desc('sum(relevance)')).show(3) #find the largest three scores and their indexes;

relGenScCl1Spl3.show()
Movies113=relGenScCl1Spl3.groupBy('tagId').sum() #sum up tag scores for all movies in it; 
#Movies113.show(5,False) # show top 3
Tags_sorted=Movies113.orderBy(desc('sum(relevance)')).show(3) #find the largest three scores and their indexes;


##sum up all the tag scores for that movie
      

