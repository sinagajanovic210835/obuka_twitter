from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
  

spark = \
  SparkSession \
    .builder \
    .appName("TwitterDe") \
    .master("local") \
    .getOrCreate() \

users = \
  spark \
    .read \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .json("hdfs://namenode:9000/user/test/users") \


users.printSchema()

metrics = users.select("username", "name", "profile_image_url", "public_metrics.followers_count", "public_metrics.following_count", "public_metrics.tweet_count")

metrics.show()

metrics.write.mode("overwrite"). \
  format("jdbc"). \
  option("user", "postgres"). \
  option("password", "postgres"). \
  option("driver", "org.postgresql.Driver"). \
  option("dbtable", "user_metrics"). \
  option("url","jdbc:postgresql://postgres:5432/postgres"). \
  save()

media = \
    spark \
        .read \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .json("hdfs://namenode:9000/user/test/media")

media.printSchema()

video = media.filter(media.type != "photo").filter(col("public_metrics").isNotNull())

video.select("variants.url").show(100)

video1 = video.select("preview_image_url", "public_metrics.view_count", "variants.url")

video1.show()

video1.write.mode("overwrite"). \
    format("jdbc"). \
    option("user", "postgres"). \
    option("password", "postgres"). \
    option("driver", "org.postgresql.Driver"). \
    option("dbtable", "videos"). \
    option("url","jdbc:postgresql://postgres:5432/postgres"). \
    save()
    