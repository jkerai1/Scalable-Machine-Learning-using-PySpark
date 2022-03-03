import numpy as np
from numpy.linalg import norm 
from pyspark.sql import Row, SparkSession
from pyspark.ml.linalg import Vectors
import matplotlib.pyplot as plt
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .master("local[2]") \
    .appName("COM6012 Assignment 1 - Q2C") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

tags = spark.read.csv("Data/tags.csv", inferSchema = "true",header = 'true') #userId,movieId,tag,timestamp
##Drop unecessary columns
tags =tags.drop(tags.timestamp)
tags = tags.drop(tags.userId)
#count number of movies with top tags from tags.csv

tag1 = "original"
tag2 = "mentor"
tag3 = "criterion"
tag4 = "storytelling"
tag5 = "weird"
tag6 = "great ending"
tag7 = "Catastrophe"
tag8 = "chase"
tag9 = "predictable"
tag10 = "talky"

tags_df_1 = tags.filter(tags["tag"].contains(tag1)).distinct().count()#172
tags_df_2 = tags.filter(tags["tag"].contains(tag2)).distinct().count()  #45
tags_df_3 = tags.filter(tags["tag"].contains(tag3)).distinct().count() #55
tags_df_4 = tags.filter(tags["tag"].contains(tag4)).distinct().count() #46
tags_df_5 = tags.filter(tags["tag"].contains(tag5)).distinct().count() #230
tags_df_6 = tags.filter(tags["tag"].contains(tag6)).distinct().count() #47
tags_df_7 = tags.filter(tags["tag"].contains(tag7)).distinct().count() #1
tags_df_8 = tags.filter(tags["tag"].contains(tag8)).distinct().count() #197
tags_df_9 = tags.filter(tags["tag"].contains(tag9)).distinct().count() #597
tags_df_10 = tags.filter(tags["tag"].contains(tag10)).distinct().count() #93

print(tags_df_1)
print(tags_df_2)
print(tags_df_3)
print(tags_df_4)
print(tags_df_5)
print(tags_df_6)
print(tags_df_7)
print(tags_df_8)
print(tags_df_9)
print(tags_df_10)