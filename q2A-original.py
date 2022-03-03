# Format of CSV:
# userId,movieId,rating,timestamp
#Timestamps represent seconds since midnight Coordinated Universal Time (UTC) of January 1, 1970.
# Note there are no nulls to deal with in this dataset
import numpy as np
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row, SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.param import Param
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.sql.functions import rand

spark = SparkSession.builder \
    .master("local[8]") \
    .appName("COM6012 Assignment 1 - Q2A-original") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

spark.catalog.clearCache() #Clear the Cache

#Sparks cross validator does not report error per fold and disregards anything but the best parameters
#Therefore in order to report results, one has to write their own cross validator class
class CrossValidatorVerbose(CrossValidator):

    def _fit(self, dataset):
        est = self.getOrDefault(self.estimator)
        epm = self.getOrDefault(self.estimatorParamMaps)
        numModels = len(epm)

        eva = self.getOrDefault(self.evaluator)
        metricName = eva.getMetricName()

        nFolds = self.getOrDefault(self.numFolds)
        seed = self.getOrDefault(self.seed)
        h = 1.0 / nFolds

        randCol = self.uid + "_rand"
        df = dataset.select("*", rand(seed).alias(randCol))
        metrics = [0.0] * numModels

        for i in range(nFolds):
            foldNum = i + 1
            print("Comparing models on fold %d" % foldNum)

            validateLB = i * h
            validateUB = (i + 1) * h
            condition = (df[randCol] >= validateLB) & (df[randCol] < validateUB)
            validation = df.filter(condition)
            train = df.filter(~condition)

            for j in range(numModels):
                paramMap = epm[j]
                model = est.fit(train, paramMap)
                
                metric = eva.evaluate(model.transform(validation, paramMap))
                metrics[j] += metric

                
                print("Parameters: %s\t%s: %f\t" % (
                    {param.name: val for (param, val) in paramMap.items()},
                    metricName, metric))

        if eva.isLargerBetter():
            bestIndex = np.argmax(metrics)
        else:
            bestIndex = np.argmin(metrics)

        bestParams = epm[bestIndex]
        bestModel = est.fit(dataset, bestParams)
        avgMetrics = [m / nFolds for m in metrics]
        bestAvg = avgMetrics[bestIndex]
        print("Best model:\nparams: %s\t%s: %f" % (
            {param.name: val for (param, val) in bestParams.items()},
            metricName, bestAvg))

        return self._copyValues(CrossValidatorModel(bestModel, avgMetrics))
#Reference: https://stackoverflow.com/questions/38874546/spark-crossvalidatormodel-access-other-models-than-the-bestmodel

df = spark.read.load("Data/ratings.csv",format = "csv", header = "true", delimiter=",") #spark has a method to load csvs directly, however I have not used it to force myself to learn lambda functions
rdd =df.rdd
#Before I can call ALS  , need to give each col the correct type, can do this using lambda functions
ratingsRDD = rdd.map(lambda p: Row(userId=int(float(p[0])), movieId=int(float(p[1])),rating=float(p[2]), timestamp=float(p[3])))

ratings = spark.createDataFrame(ratingsRDD)
ratings_split = ratings
#ratings_split = ratings.sample(False, 0.1) #sample the data for testing, comment this out after code is finished
#ratings_split.cache() #Reference: https://databricks.com/session/getting-the-best-performance-with-pyspark


#Base Strategy
als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop") # Drop choosen here for cold start strategy


##start a pipeline
pipeline = Pipeline(stages=[als])
#Define evaluators
modelEvaluator=RegressionEvaluator(labelCol="rating",predictionCol="prediction") # by default this will use rmse
evaluator2 = RegressionEvaluator(metricName="mae", labelCol="rating",predictionCol="prediction")
#Reference: https://stackoverflow.com/questions/45420112/cross-validation-in-pysparks

#Build grid of parameters
paramGrid = ParamGridBuilder().addGrid(als.regParam, [0.1, 0.01]).addGrid(als.maxIter, [10,20]).build() #train 4 different  versions, Im using an extra version so I can dicuss in more detail in report


#Crossvalidate for RMSE
crossval1 = CrossValidatorVerbose(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=modelEvaluator,
                          numFolds=3,seed = 1234)
#Crossvalidate for MAE                          
crossval1_1 = CrossValidatorVerbose(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator2,
                          numFolds=3,seed = 1234)
                          

#Give all models the same training set
cvModel1 = crossval1.fit(ratings_split)
cvModel1_1 = crossval1_1.fit(ratings_split)

###### Examiner ignore#########################


#####Make Splits#######
#(training, test,val) = ratings_split.randomSplit([0.7, 0.2,0.1],1999) # first split ive choosen,fixed seed for reproducability

#crossval2 = CrossValidatorVerbose(estimator=pipeline,
            #              estimatorParamMaps=paramGrid2,
             #             evaluator=modelEvaluator,
              #            numFolds=3)
                          
#crossval2_2 = CrossValidatorVerbose(estimator=pipeline,
               #           estimatorParamMaps=paramGrid2,
                #          evaluator=evaluator2,
                 #         numFolds=3)                  


#crossval3 = CrossValidatorVerbose(estimator=pipeline,
                  #        estimatorParamMaps=paramGrid3,
                   #       evaluator=modelEvaluator,
                    #      numFolds=3)

#crossval3_3 = CrossValidatorVerbose(estimator=pipeline,
                     #     estimatorParamMaps=paramGrid3,
                      #    evaluator=evaluator2,
                       #   numFolds=3)    
                       
#cvModel2 = crossval2.fit(training)
#cvModel2_2 = crossval2_2.fit(training)
#cvModel3 = crossval3.fit(training)
#cvModel3_3 = crossval3_3.fit(training)

#prediction_cvmodel_1 = cvModel1.transform(test)
#prediction_cvmodel_11 = cvModel1.transform(val)

#prediction_cvmodel1_1 = cvModel1_1.transform(test)
#prediction_cvmodel1_11 = cvModel1_1.transform(val)


#prediction_cvmodel_2 = cvModel2.transform(test)
#prediction_cvmodel_22 = cvModel2.transform(val)

#prediction_cvmodel2_2 = cvModel2_2.transform(test)
#prediction_cvmodel2_22 = cvModel2_2.transform(val)

#prediction_cvmodel_3 = cvModel3.transform(test)#
#prediction_cvmodel_33 = cvModel3.transform(val)

#prediction_cvmodel3_3 = cvModel3_3.transform(test)
#prediction_cvmodel3_33 = cvModel3_3.transform(val)


#print("cross validation stats:")

#rmse1 = evaluator1.evaluate(prediction_cvmodel_1)
#mae1 = evaluator2.evaluate(prediction_cvmodel_1)

#rmse11 =evaluator1.evaluate(prediction_cvmodel_11)
#mae11 =evaluator2.evaluate(prediction_cvmodel_11)

#rmse2 =evaluator1.evaluate(prediction_cvmodel_2)
#mae2 = evaluator2.evaluate(prediction_cvmodel_2)

#rmse22 = evaluator1.evaluate(prediction_cvmodel_22)
#rmse22 = evaluator2.evaluate(prediction_cvmodel_22)

#rmse3 = evaluator1.evaluate(prediction_cvmodel_3)
#mae3 = evaluator2.evaluate(prediction_cvmodel_3)
#
#rmse33 = evaluator1.evaluate(prediction_cvmodel_33)
#mae33 = evaluator2.evaluate(prediction_cvmodel_33)

#print("Model 1: (training on test)")
#print(rmse1)
#print(mae1)

#print("Model 1: (training on val)")
#print(rmse11)
#print(mae11)

#print("Model 2: (training on test)")
#print(rmse2)
#print(mae2)

#print("Model 2: (training on val)")
#print(rmse22)
#print(mae22)

#print("Model 3: (training on test)")
#print(rmse3)
#print(mae3)

#print("Model 3: (training on val)")
#print(rmse33)
#print(mae33)


#report Error per fold




