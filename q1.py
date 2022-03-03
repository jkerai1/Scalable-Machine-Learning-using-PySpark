#######Import List####################################
import numpy as np
import re
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

###########################Start#######################
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("COM6012 ML Intro") \
    .getOrCreate()
    
sc = spark.sparkContext
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)

spark = SparkSession(sc)
##########################################################

#Load Dataset
logFile=spark.read.text("Data/NASA_access_log_Jul95.gz").cache() # Use July dataset now

#Find out the average number of requests on each four hours of a day of July 1995 by the local time


#Specify the Schema using Regex
#Reference: https://opensource.com/article/19/5/log-data-apache-spark
host_pattern=r'(^\S+\.[\S+\.]+\S+)\S'
### The format in the file is [01/Jul/1995:00:00:01 -0400]
date_pattern = r'\[((\d{2}/\w{3}/\d{4}))'
time_pattern = r'(\d{2}:\d{2}:\d{2} -\d{4})]' # I have matched the UTC code but this is not necessary
method_uri=r'\"(\S+)\s(\S+)\s*(\S*)\"'   
status_pattern=r'\s(\d{3})\s'
content_size_pattern=r'\s(\d+)$'


##Use regex to extract the data and assign names for each column
logs_df=logFile.select(regexp_extract('value',host_pattern,1).alias('host'),
                       regexp_extract('value',date_pattern,1).alias('date'),
                       regexp_extract('value',time_pattern,1).alias('time'),
                       regexp_extract('value',method_uri,1).alias('method'),
                       regexp_extract('value',method_uri,2).alias('endpoint'),
                       regexp_extract('value',method_uri,3).alias('protocol'),
                       regexp_extract('value',status_pattern,1).cast('integer').alias('status'),
                       regexp_extract('value',content_size_pattern,1).cast('integer').alias('content_size'))

#Get rid of Null Entries
logs_df = logs_df.na.drop()

#Months are given as 3 letter codes
logs_july = logs_df.filter(logs_df['date'].contains("/Jul/1995")) #filter out any date that isnt July just in case
days = logs_july.select(logs_july['date']).distinct().count() # fixing no of days for july, as there are missing days in july , there are only 28 distinct days
print("Unique days: ")
print(days)
#Note we only need to check hours i.e 00 - 03, 04-07, 08-11,12-3,4 -7, 8 -11, all minutes within that set are included
#I am also ignoring the timezone
#I could write a loop for the code below but doesn't really simplify the code in anyway
logs_july_block1 = logs_july.filter(( logs_july['time'].startswith("00:") | logs_july['time'].startswith("01:") | logs_july['time'].startswith("02:") | logs_july['time'].startswith("03:")))
logs_july_block2 =logs_july.filter(( logs_july['time'].startswith("04:") | logs_july['time'].startswith("05:") | logs_july['time'].startswith("06:") | logs_july['time'].startswith("07:")))
logs_july_block3 =logs_july.filter(( logs_july['time'].startswith("08:") | logs_july['time'].startswith("09:") | logs_july['time'].startswith("10:") | logs_july['time'].startswith("11:")))
logs_july_block4 =logs_july.filter(( logs_july['time'].startswith("12:") | logs_july['time'].startswith("13:") | logs_july['time'].startswith("14:") | logs_july['time'].startswith("15:")))
logs_july_block5 =logs_july.filter(( logs_july['time'].startswith("16:") | logs_july['time'].startswith("17:") | logs_july['time'].startswith("18:") | logs_july['time'].startswith("19:")))
logs_july_block6 =logs_july.filter(( logs_july['time'].startswith("20:") | logs_july['time'].startswith("21:") | logs_july['time'].startswith("22:") | logs_july['time'].startswith("23:")))

block1count = logs_july_block1.count()/days #7000.642857142857
block2count = logs_july_block2.count()/days #5428.892857142857
block3count = logs_july_block3.count()/days #14319.42857142857
block4count = logs_july_block4.count()/days #17195.5
block5count = logs_july_block5.count()/days #12956.75
block6count = logs_july_block6.count()/days #9955.5

#debugging purposes
print(block1count)
print(block2count)
print(block3count)
print(block4count)
print(block5count)
print(block6count)

#Plot the bar chart
#Reference: https://matplotlib.org/3.1.3/gallery/lines_bars_and_markers/bar_stacked.html#sphx-glr-gallery-lines-bars-and-markers-bar-stacked-py
BlockList = [block1count,block2count,block3count,block4count,block5count,block6count]
TimeList = ["00:00:00 - 03:59:59"," 04:00:00-07:59:59","08:00:00-11:59:59","12:00:00-15:59:59","16:00:00-19:59:59","20:00:00-23:59:59"]
p1 = plt.bar(TimeList,BlockList)
plt.ylabel("Average Number of Requests", size = 25)
plt.xlabel("Time Slots", size = 25)
plt.xticks(size = 20)
plt.title("Average number of requests on each four hours of a day in July 1995", size = 30)
#plt.savefig("/home/acp19jk/figure1.png") ##Examiner wont have access to my directory
#plt.savefig('figure_1A_Final.png', bbox_inches = "tight") # Reference: https://stackoverflow.com/questions/6774086/why-is-my-xlabel-cut-off-in-my-matplotlib-plot
plt.show()





#logs_df.show(10,False) #for debugging


#Q1C

#Dropping nulls will have affected the results obtained for this question. any valid html request should have a content size however.
#Ignoring File path, can use .endswith but contain will match as .html is always at the end because it is a file type  415971 vs 416078 in the total count, for the top 20 files this will not matter
commonFiles=logs_df.filter(logs_df['endpoint'].contains(".html")) 
commonFiles = commonFiles.groupby("endpoint").count() #Count all records for amount of files

commonFilesList = commonFiles.orderBy(desc('count')).limit(20)
commonFilesList.show(20, False)


#Q1D
#First we need to convert the filtered pyspark df into a list of strings containing the file names and an array with the counts for each file 
#Reference: https://subscription.packtpub.com/book/big_data_and_business_intelligence/9781788474221/2/ch02lvl1sec23/converting-a-pyspark-dataframe-to#-an-array

#Pandas is the easiest way to achieve a list of strings. Note this could be done in one line but I have split it
commonFilesList = commonFilesList.toPandas()
labels = commonFilesList.iloc[:,0].tolist() 
counts = commonFilesList["count"].to_numpy()
#Reference: https://stackoverflow.com/questions/23748995/pandas-dataframe-column-to-list


#Reference: https://pythonspot.com/matplotlib-pie-chart/
colours = ['gold', 'yellowgreen', 'lightcoral', 'lightskyblue'] # make it look nice
plt.pie(counts, labels=labels, colors=colours,
autopct='%1.1f%%', shadow=True, startangle=180)
plt.legend(loc='upper center', bbox_to_anchor=(1.45, 0.8), shadow=True, ncol=1)
#Reference: https://pythonspot.com/matplotlib-legend/
plt.title("Most Common .html files", size = 30)
#plt.savefig("/home/acp19jk/figure2.png") #Examiner wont have access to my directory
#plt.savefig('figure_1D_final.png', bbox_inches = "tight") # Reference: https://stackoverflow.com/questions/6774086/why-is-my-xlabel-cut-off-in-my-matplotlib-plot
plt.show()

#logs_df.show(10,False) #for debugging
spark.stop()
