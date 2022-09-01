from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = \
    SparkSession \
      .builder \
      .appName("TwitterDe") \
      .master("local") \
      .getOrCreate()

tweets = \
    spark \
      .read \
      .option("inferSchema", "true") \
      .option("header", "true") \
      .json("hdfs://namenode:9000/user/test/clean_data") \


 

annot = tweets.select(col("username"), explode(col("context_annotations"))).cache()

# domains
domain = annot.select("col.domain.description", "col.domain.name") \
           .groupBy("description", "name").count()

domain.write.mode("overwrite") \
      .format("jdbc") \
      .option("dbtable","twitter_domains") \
      .option("user", "postgres") \
      .option("password", "postgres") \
      .option("driver", "org.postgresql.Driver") \
      .option("url","jdbc:postgresql://postgres:5432/postgres") \
      .save()

# entities
entity = annot \
    .select("col.entity.name") \
    .groupBy("name") \
    .count()

entity.write.mode("overwrite") \
    .format("jdbc") \
    .option("dbtable","entities") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("url","jdbc:postgresql://postgres:5432/postgres") \
    .save()    

annot.unpersist()

entities = tweets.filter(col("entities").isNotNull()) \
    .select("username", "entities").cache()

# annotations
annotations = entities.filter(col("entities.annotations").isNotNull()) \
    .select(explode(col("entities.annotations"))) \
    .select("col.type", "col.normalized_text") \
    .groupBy("type", "normalized_text").count()

annotations.write.mode("overwrite") \
    .format("jdbc") \
    .option("dbtable","twitter_annotations") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("url","jdbc:postgresql://postgres:5432/postgres") \
    .save()

# cashtags
cashtags = entities.filter(col("entities.cashtags").isNotNull()) \
    .select(explode(col("entities.cashtags"))) \
    .select("col.tag") \
    .groupBy("tag").count() \


cashtags.write.mode("overwrite") \
    .format("jdbc") \
    .option("dbtable","twitter_cashtags") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("url","jdbc:postgresql://postgres:5432/postgres") \
    .save()

# hashtags
hashtags = entities.filter(col("entities.hashtags").isNotNull()) \
    .select(explode(col("entities.hashtags"))) \
    .select("col.tag") \
    .groupBy("tag") \
    .count()

hashtags.write.mode("overwrite") \
    .format("jdbc") \
    .option("dbtable","twitter_hashtags") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("url","jdbc:postgresql://postgres:5432/postgres") \
    .save()

# mentions
mentions = entities.filter(col("entities.mentions").isNotNull()) \
    .select(explode(col("entities.mentions"))) \
    .select("col.username") \
    .groupBy("username") \
    .count()


mentions.write.mode("overwrite") \
    .format("jdbc") \
    .option("dbtable","twitter_mentions") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("url","jdbc:postgresql://postgres:5432/postgres") \
    .save()

entities.unpersist()

# geo coordinates
geo = tweets.filter(col("geo.coordinates.coordinates").isNotNull()) \
    .select(col("username"), \
            col("text"), \
            col("geo.coordinates.coordinates").getItem(0).alias("longitude"), \
            col("geo.coordinates.coordinates").getItem(1).alias("latitude")) \
            .distinct() \
            
geo.write.mode("overwrite") \
    .format("jdbc") \
    .option("dbtable","twitter_geo_locations") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("url","jdbc:postgresql://postgres:5432/postgres") \
    .save()

publicMetrics = tweets.filter(col("public_metrics").isNotNull()) \
    .select("username", "text", "public_metrics").cache()

# likes
likes = publicMetrics \
      .select(col("username"), col("text"), col("public_metrics.like_count").alias("likes_count")) \
      .orderBy(col("likes_count").desc())

likes.write.mode("overwrite") \
    .format("jdbc") \
    .option("dbtable","like_count") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("url","jdbc:postgresql://postgres:5432/postgres") \
    .save()

# quotes
quotes = publicMetrics \
    .filter(col("public_metrics.quote_count") > 0) \
    .select(col("username"), col("text"), col("public_metrics.quote_count").alias("quotes_count")) \
    .orderBy(col("quotes_count").desc())

quotes.write.mode("overwrite") \
    .format("jdbc") \
    .option("dbtable","quote_count") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("url","jdbc:postgresql://postgres:5432/postgres") \
    .save()

# replies
replies = publicMetrics \
    .filter(col("public_metrics.reply_count") > 0) \
    .select(col("username"), col("text"), col("public_metrics.reply_count").alias("replies_count")) \
    .orderBy(col("replies_count").desc())

replies.write.mode("overwrite") \
    .format("jdbc") \
    .option("dbtable","reply_count") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("url","jdbc:postgresql://postgres:5432/postgres") \
    .save()

# retweets
retweets = publicMetrics \
    .filter(col("public_metrics.retweet_count") > 0) \
    .select(col("username"), col("text"), col("public_metrics.retweet_count").alias("retweets_count")) \
    .orderBy(col("retweets_count").desc()) \
    .distinct()

retweets.write.mode("overwrite") \
    .format("jdbc") \
    .option("dbtable","retweet_count") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("url","jdbc:postgresql://postgres:5432/postgres") \
    .save()

publicMetrics.unpersist()
