from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# clean Batch Data - remove duplicates and add username and name fields

spark = \
  SparkSession \
    .builder \
    .appName("TwitterDe") \
    .master("local") \
    .getOrCreate() \

tweets = \
    spark \
        .read \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .json("hdfs://namenode:9000/user/test/splits") \

users = \
    spark \
      .read \
      .option("inferSchema", "true") \
      .option("header", "true") \
      .json("hdfs://namenode:9000/user/test/users") \
      .select(col("id").alias("uid"), \
              col("username"), \
              col("name"))

temp = tweets \
           .select(col("id").cast(LongType()).alias("id1"), col("last_modified").cast(LongType())) \
           .groupBy("id1") \
           .agg(max(col("last_modified")).alias("l_modified")) \

clean_data = tweets \
                .join(temp, (col("id") == col("id1")) & (col("last_modified") == col("l_modified")), "inner") \
                .drop("id1", "l_modified")

tweetsWithUsername = clean_data.join(users, col("author_id") == col("uid")).drop("uid")

tweetsWithUsername \
    .repartition(6) \
    .write \
    .json("hdfs://namenode:9000/user/test/clean_data")

    