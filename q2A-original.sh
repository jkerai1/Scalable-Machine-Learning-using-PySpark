#!/bin/bash
#$ -l h_rt=2:00:00  #time needed
#$ -pe smp 8 #number of cores
#$ -l rmem=16G #number of mem
#$ -o Assignment_q2_orginal.txt #This is where your output and errors are logged.
#$ -j y # normal and error outputs into a single file (the file above)
#$ -cwd # Run job from current directory

module load apps/java/jdk1.8.0_102/binary

module load apps/python/conda

source activate myspark

spark-submit --driver-memory 40g --executor-memory 2g --master local[8] --conf spark.driver.maxResultSize=4g  q2A-original.py
