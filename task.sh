#!/bin/bash
/bin/date
#
#export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_72.jdk/Contents/Home
export BOOK_HOME=/home/my/a
#export SPARK_HOME=/Users/mparsian/spark-2.0.0-bin-hadoop2.6
#export SPARK_MASTER=spark://localhost:7077
export APP_JAR=$BOOK_HOME/tryspark-0.0.1.jar
#export HADOOP_USER_NAME=
#
#
k=4
product="file://$BOOK_HOME/input3000.txt"
countryip="file://$BOOK_HOME/CountryIP.csv"
countryname="file://$BOOK_HOME/CountryName.csv"
prog=tryspark.SparkSQL
#
spark-submit --class $prog \
    --master local \
    --num-executors 2 \
    --driver-memory 1g \
    --executor-memory 2g \
    --executor-cores 2 \
    $APP_JAR $product $countryip $countryname "" "" "" ""