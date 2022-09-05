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

df = \
    spark \
        .read \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .json("hdfs://namenode:9000/user/test/includes")

users = df.select("*",explode("users"))
        #   .select("col.username", "col.name","col.id","col.created_at","col.description","col.entities", \
        #        "col.location","col.pinned_tweet_id","col.profile_image_url","col.protected","col.public_metrics", \
        #        "col.url","col.verified") \
        #   .groupBy("username") \
        #   .agg(first(col("name")), first(col("id")).alias("id"),first(col("created_at")).alias("created_at"),first(col("description")).alias("description"), \
        #        first(col("entities")).alias("entities"), first(col("location")).alias("location"), \
        #        first(col("pinned_tweet_id")).alias("pinned_tweet_id"), first(col("profile_image_url")).alias("profile_image_url"), \
        #        first(col("protected")).alias("protected"),first(col("public_metrics")).alias("public_metrics"), first(col("url")).alias("url"), \
        #        first(col("verified")).alias("verified"))

users.show()               

# media = df.select(explode(col("media"))) \
#           .select("col.alt_text","col.duration_ms","col.height", "col.media_key", \
#           "col.preview_image_url", "col.public_metrics", "col.type", "col.url", "col.variants", "col.width") \
#           .distinct()

# users \
#     .repartition(4) \
#     .write \
#     .json("hdfs://namenode:9000/user/test/users")

# media \
#     .repartition(2) \
#     .write \
#     .json("hdfs://namenode:9000/user/test/media")

users.printSchema()

# media.printSchema()

