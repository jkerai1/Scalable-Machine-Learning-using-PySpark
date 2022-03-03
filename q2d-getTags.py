#Reference: http://files.grouplens.org/datasets/movielens/ml-25m-README.html
#The tag genome is a data structure that contains tag relevance scores for movies. The structure is a dense matrix: each movie in the genome has a value for every tag in the genome.
#genome-scores.csv - movieId (int) ,tagId (int),relevance (float)
#genome-tags.csv - tagId(int),tag (string)
#Reference: Jesse Vig, Shilad Sen, and John Riedl. 2012. The Tag Genome: Encoding Community Knowledge to Support Novel Interaction. ACM Trans. Interact. Intell. Syst.

import numpy as np
from numpy.linalg import norm 
#from pyspark.ml.evaluation import RegressionEvaluator
#from pyspark.ml.recommendation import ALS
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



genome_tags = spark.read.csv("Data/genome-tags.csv", inferSchema = "true",header = 'true')

tag_id =[None] * 27
####als1
##9 tags to return
tag_id[0] = 742 #original
tag_id[1] = 646 #mentor
tag_id[2] = 270 #great ending
tag_id[3] = 742#
tag_id[4] = 972
tag_id[5] = 1104
tag_id[6] = 742 
tag_id[7] = 646
tag_id[8] = 468 



MovieId1= genome_tags.filter(genome_tags['tagId'].contains(tag_id[0])) #1128 tags per film, need top 3
MovieId1=MovieId1.orderBy(desc('tag')).show(1,False)


MovieId2= genome_tags.filter(genome_tags['tagId'].contains(tag_id[1])) #1128 tags per film, need top 3
MovieId2=MovieId2.orderBy(desc('tag')).show(1,False)

MovieId3= genome_tags.filter(genome_tags['tagId'].contains(tag_id[2])) #1128 tags per film, need top 3
MovieId3=MovieId3.orderBy(desc('tag')).show(1,False)

print("cluster2:")
MovieId4= genome_tags.filter(genome_tags['tagId'].contains(tag_id[3])) #1128 tags per film, need top 3
MovieId4=MovieId4.orderBy(desc('tag')).show(1,False)

MovieId5= genome_tags.filter(genome_tags['tagId'].contains(tag_id[4])) #1128 tags per film, need top 3
MovieId5=MovieId5.orderBy(desc('tag')).show(1,False)

MovieId6= genome_tags.filter(genome_tags['tagId'].contains(tag_id[5])) #1128 tags per film, need top 3

MovieId6=MovieId6.orderBy(desc('tag')).show(1,False)
print("cluster 3:")

MovieId7= genome_tags.filter(genome_tags['tagId'].contains(tag_id[6])) #1128 tags per film, need top 3
MovieId7=MovieId7.orderBy(desc('tag')).show(1,False)

MovieId8= genome_tags.filter(genome_tags['tagId'].contains(tag_id[7])) #1128 tags per film, need top 3
MovieId8=MovieId8.orderBy(desc('tag')).show(1,False)

MovieId9= genome_tags.filter(genome_tags['tagId'].contains(tag_id[8])) #1128 tags per film, need top 3
MovieId9=MovieId9.orderBy(desc('tag')).show(1,False)


####als2

tag_id[9] = 742 
tag_id[10] = 972
tag_id[11] = 646 
tag_id[12] = 742
tag_id[13] = 646
tag_id[14] = 188
tag_id[15] = 742
tag_id[16] = 646
tag_id[17] = 195



MovieId1= genome_tags.filter(genome_tags['tagId'].contains(tag_id[9])) #1128 tags per film, need top 3
MovieId1=MovieId1.orderBy(desc('tag')).show(1,False)


MovieId2= genome_tags.filter(genome_tags['tagId'].contains(tag_id[10])) #1128 tags per film, need top 3
MovieId2=MovieId2.orderBy(desc('tag')).show(1,False)

MovieId3= genome_tags.filter(genome_tags['tagId'].contains(tag_id[11])) #1128 tags per film, need top 3
MovieId3=MovieId3.orderBy(desc('tag')).show(1,False)

print("cluster2:")
MovieId4= genome_tags.filter(genome_tags['tagId'].contains(tag_id[12])) #1128 tags per film, need top 3
MovieId4=MovieId4.orderBy(desc('tag')).show(1,False)

MovieId5= genome_tags.filter(genome_tags['tagId'].contains(tag_id[13])) #1128 tags per film, need top 3
MovieId5=MovieId5.orderBy(desc('tag')).show(1,False)

MovieId6= genome_tags.filter(genome_tags['tagId'].contains(tag_id[14])) #1128 tags per film, need top 3

MovieId6=MovieId6.orderBy(desc('tag')).show(1,False)
print("cluster 3:")

MovieId7= genome_tags.filter(genome_tags['tagId'].contains(tag_id[15])) #1128 tags per film, need top 3
MovieId7=MovieId7.orderBy(desc('tag')).show(1,False)

MovieId8= genome_tags.filter(genome_tags['tagId'].contains(tag_id[16])) #1128 tags per film, need top 3
MovieId8=MovieId8.orderBy(desc('tag')).show(1,False)

MovieId9= genome_tags.filter(genome_tags['tagId'].contains(tag_id[17])) #1128 tags per film, need top 3
MovieId9=MovieId9.orderBy(desc('tag')).show(1,False)
####als3

tag_id[18] = 742 #original
tag_id[19] = 646 #mentor
tag_id[20] = 445 #great ending
tag_id[21] = 742#
tag_id[22] = 807
tag_id[23] = 646
tag_id[24] = 270 
tag_id[25] = 742
tag_id[26] = 1008 



MovieId1= genome_tags.filter(genome_tags['tagId'].contains(tag_id[18])) #1128 tags per film, need top 3
MovieId1=MovieId1.orderBy(desc('tag')).show(1,False)


MovieId2= genome_tags.filter(genome_tags['tagId'].contains(tag_id[19])) #1128 tags per film, need top 3
MovieId2=MovieId2.orderBy(desc('tag')).show(1,False)

MovieId3= genome_tags.filter(genome_tags['tagId'].contains(tag_id[20])) #1128 tags per film, need top 3
MovieId3=MovieId3.orderBy(desc('tag')).show(1,False)

print("cluster2:")
MovieId4= genome_tags.filter(genome_tags['tagId'].contains(tag_id[21])) #1128 tags per film, need top 3
MovieId4=MovieId4.orderBy(desc('tag')).show(1,False)

MovieId5= genome_tags.filter(genome_tags['tagId'].contains(tag_id[22])) #1128 tags per film, need top 3
MovieId5=MovieId5.orderBy(desc('tag')).show(1,False)

MovieId6= genome_tags.filter(genome_tags['tagId'].contains(tag_id[23])) #1128 tags per film, need top 3

MovieId6=MovieId6.orderBy(desc('tag')).show(1,False)
print("cluster 3:")

MovieId7= genome_tags.filter(genome_tags['tagId'].contains(tag_id[24])) #1128 tags per film, need top 3
MovieId7=MovieId7.orderBy(desc('tag')).show(1,False)

MovieId8= genome_tags.filter(genome_tags['tagId'].contains(tag_id[25])) #1128 tags per film, need top 3
MovieId8=MovieId8.orderBy(desc('tag')).show(1,False)

MovieId9= genome_tags.filter(genome_tags['tagId'].contains(tag_id[26])) #1128 tags per film, need top 3
MovieId9=MovieId9.orderBy(desc('tag')).show(1,False)


