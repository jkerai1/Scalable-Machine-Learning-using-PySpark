#!/bin/bash
#$ -l h_rt=2:00:00  #time needed
#$ -pe smp 2 #number of cores
#$ -l rmem=4G #number of mem
#$ -o Assignment_q1.txt,Assignment_q1_figure1.png,Assignment_q1_figure2.png #This is where your output and errors are logged.
#$ -j y # normal and error outputs into a single file (the file above)
#$ -cwd # Run job from current directory

module load apps/java/jdk1.8.0_102/binary

module load apps/python/conda

source activate myspark

spark-submit q1.py
