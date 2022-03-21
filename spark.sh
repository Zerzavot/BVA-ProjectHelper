sudo apt update
sudo apt -y upgrade
sudo apt -y install default-jre
sudo apt -y install default-jdk

sudo wget https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
tar xvf spark-3.2.1-bin-hadoop3.2.tgz
sudo mv spark-3.2.1-bin-hadoop3.2/ /opt/spark 

sudo cat <<EOF >>~/.bashrc
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
EOF

source ~/.bashrc

sudo mkdir /home/data
sudo mkdir /home/code
sudo wget https://raw.githubusercontent.com/mmcky/nyu-econ-370/master/notebooks/data/book-war-and-peace.txt -P /home/data/

sudo touch /home/code/word_count.py

sudo cat <<EOF >>/home/code/word_count.py
from __future__ import print_function

import sys
from operator import add
import os

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    textfile = spark.read.text("/home/data/book-war-and-peace.txt")

    lines = textfile.rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

spark.stop()
EOF




