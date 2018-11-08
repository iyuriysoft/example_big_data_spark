## Spark Example

There are three tasks:

5.1 Select top 10 most frequently purchased categories (using only Product)

5.2 Select top 10 most frequently purchased product in each category (using only Product)

6.3 Select top 10 countries with the highest money spending (using Product, CountryName, CountryIP)


The tasks are implemented in three different approaches: RDD, Dataframe, SQL

### Input Data

Input data are three files:

[input3000.txt](https://github.com/iyuriysoft/big_data_sketch/blob/master/InputData/input3000.txt)

[CountryIP.csv](https://github.com/iyuriysoft/big_data_sketch/blob/master/InputData/CountryIP.csv)

[CountryName.csv](https://github.com/iyuriysoft/big_data_sketch/blob/master/InputData/CountryName.csv)


Output data Should be in MySql database

### Output data in MySql tables like these:

```sql
CREATE TABLE `table51` (
  `category` text,
  `cnt` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `table52` (
  `name` text,
  `category` text,
  `cnt` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `table63` (
  `sump` double DEFAULT NULL,
  `IP` text,
  `countryName` text
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

```

---
Run Spark example:

```bash
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
```
