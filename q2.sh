#!/bin/bash
#$ -l h_rt=4:00:00  #time needed
#$ -pe smp 4 #number of cores
#$ -l rmem=32G #number of mem
#$ -o Assignment_q2_v9.txt #This is where your output and errors are logged.
#$ -j y # normal and error outputs into a single file (the file above)
#$ -M jkerai1@shef.ac.uk #Notify you by email, remove this line if you don't like
#$ -m ea #Email you when it finished or aborted
#$ -cwd # Run job from current directory

module load apps/java/jdk1.8.0_102/binary

module load apps/python/conda

source activate myspark

spark-submit  --driver-memory 10g --executor-memory 10g --master local[4] --conf  spark.driver.maxResultSize=10g   q2A.py