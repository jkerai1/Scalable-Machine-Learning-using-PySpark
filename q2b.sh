#!/bin/bash
#$ -l h_rt=2:00:00  #time needed
#$ -pe smp 2 #number of cores
#$ -l rmem=2G #number of mem
#$ -o COM6012_Lab1.output #This is where your output and errors are logged.
#$ -j y # normal and error outputs into a single file (the file above)
#$ -M jkerai1@shef.ac.uk #Notify you by email, remove this line if you don't like
#$ -m ea #Email you when it finished or aborted
#$ -cwd # Run job from current directory

module load apps/java/jdk1.8.0_102/binary

module load apps/python/conda

source activate myspark

spark-submit q2b.py