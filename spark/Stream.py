from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

schema = StructType( \
    [ \
      StructField("attachments", StructType( \
        [ \
          StructField("media_keys", ArrayType(StringType()), True), \
          StructField("poll_ids", ArrayType(StringType()), True) \
        ]), True), \
      StructField("author_id", StringType(), True), \
      StructField("context_annotations", ArrayType( \
        StructType( \
          [ \
            StructField("domain", StructType( \
              [ \
                StructField("description", StringType(), True), \
                StructField("id", StringType(), True), \
                StructField("name", StringType(), True) \
              ] \
            ), True), \
            StructField("entity", StructType( \
              [ \
                StructField("description", StringType(), True), \
                StructField("id", StringType(), True), \
                StructField("name", StringType(), True) \
              ] \
            ), True) \
          ] \
        ) \
      ), True), \
      StructField("conversation_id", StringType(), True), \
      StructField("created_at", StringType(), True), \
      StructField("entities", StructType([ \
        StructField("annotations", ArrayType( \
          StructType( \
            [ \
              StructField("end", LongType(), True), \
              StructField("normalized_text", StringType(), True), \
              StructField("probability", DoubleType(), True), \
              StructField("start", LongType(), True), \
              StructField("type", StringType(), True) \
            ] \
          ) \
        ), True), \
        StructField("cashtags", ArrayType( \
          StructType( \
            [ \
              StructField("end", LongType(), True), \
              StructField("start", LongType(), True), \
              StructField("tag", StringType(), True) \
            ] \
          ) \
        ), True), \
        StructField("hashtags", ArrayType( \
          StructType( \
            [ \
              StructField("end", LongType(), True), \
              StructField("start", LongType(), True), \
              StructField("tag", StringType(), True) \
            ] \
          ) \
        ), True), \
        StructField("mentions", ArrayType( \
          StructType( \
            [ \
              StructField("end", LongType(), True), \
              StructField("id", StringType(), True), \
              StructField("start", LongType(), True), \
              StructField("username", StringType(), True) \
             ] \
          ) \
        ), True), \
        StructField("urls", ArrayType( \
          StructType( \
            [ \
            StructField("description", StringType(), True), \
            StructField("display_url", StringType(), True), \
            StructField("end", LongType(), True), \
            StructField("expanded_url", StringType(), True), \
            StructField("images", ArrayType( \
              StructType([ \
                StructField("height", LongType(), True), \
                StructField("url", StringType(), True), \
                StructField("width", LongType(), True) \
            ]) \
            ), True), \
            StructField("media_key", StringType(), True), \
            StructField("start", LongType(), True), \
            StructField("status", LongType(), True), \
            StructField("title", StringType(), True), \
            StructField("unwound_url", StringType(), True), \
            StructField("url", StringType(), True) \
            ]) \
        ), True) \
      ]), True), \
      StructField("geo", StructType( \
          [ \
          StructField("coordinates", StructType( \
            [ \
              StructField("coordinates", ArrayType(DoubleType()), True), \
              StructField("type", StringType(), True) \
            ] \
          ), True), \
          StructField("place_id", StringType(), True) \
          ] \
      ), True), \
      StructField("id", StringType(), True), \
      StructField("in_reply_to_user_id", StringType(), True), \
      StructField("lang", StringType(), True), \
      StructField("last_modified", StringType(), True), \
      StructField("possibly_sensitive", BooleanType(), True), \
      StructField("public_metrics", StructType( \
         [ \
          StructField("like_count", LongType(), True), \
          StructField("quote_count", LongType(), True), \
          StructField("reply_count", LongType(), True), \
          StructField("retweet_count", LongType(), True) \
         ] \
      ), True), \
      StructField("referenced_tweets", ArrayType(StructType( \
        [ \
          StructField("id", StringType(), True), \
          StructField("type", StringType(), True) \
        ] \
      )), True), \
      StructField("reply_settings", StringType(), True), \
      StructField("source", StringType(), True), \
      StructField("text", StringType(), True), \
      StructField("wintheld", StructType( \
        [ \
          StructField("copyright", BooleanType(), True), \
          StructField("country_codes", ArrayType(StringType(), True), True) \
        ] \
      ), True), \
      StructField("username", StringType(), True) \
    ] \
  )

spark = SparkSession \
    .builder \
    .appName("ZadatakDE") \
    .config("spark.master", "local") \
    .getOrCreate() \

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("subscribe", "twitter1") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "latest") \
    .load()

  
   

kafka = df.selectExpr("cast (key as string)","cast (value as string)") \
                  .select(col("key"), from_json(col("value") \
                  .cast(StringType()), schema) \
                  .alias("value")) \

def batchFunction(batch, bid):
      geo = batch.filter(col("value.geo.coordinates.coordinates").isNotNull()) \
            .select(col("value.username"), \
              col("value.text"), \
              col("value.geo.coordinates.coordinates").getItem(0).alias("longitude"), \
              col("value.geo.coordinates.coordinates").getItem(1).alias("latitude") \
            )            
      geo.write \
          .format("jdbc") \
          .option("user","postgres") \
          .option("password","postgres") \
          .option("driver","org.postgresql.Driver") \
          .option("url","jdbc:postgresql://postgres:5432/postgres") \
          .option("dbtable","geo_kafka") \
          .mode("append") \
          .save()

      entities = batch.filter(col("value.entities").isNotNull()) \
                .select(col("value.username").alias("username"), \
                        col("value.entities").alias("entities"))

      # annotations
      annotations = entities \
                      .filter(col("entities.annotations").isNotNull()) \
                      .select(explode(col("entities.annotations"))) \
                      .select("col.type", "col.normalized_text") \
                      .groupBy("type", "normalized_text").count()
      annotations \
          .write \
          .mode("append") \
          .format("jdbc") \
          .option("user","postgres") \
          .option("password","postgres") \
          .option("driver","org.postgresql.Driver") \
          .option("url","jdbc:postgresql://postgres:5432/postgres") \
          .option("dbtable","kafka_twitter_annotations") \
          .save()

      # cashtags
      cashtags = entities.filter(col("entities.cashtags").isNotNull()) \
                .select(explode(col("entities.cashtags"))) \
                .select("col.tag") \
                .groupBy("tag") \
                .count()
      cashtags \
              .write \
              .mode("append") \
              .format("jdbc") \
              .option("user","postgres") \
              .option("password","postgres") \
              .option("driver","org.postgresql.Driver") \
              .option("url","jdbc:postgresql://postgres:5432/postgres") \
              .option("dbtable","kafka_twitter_cashtags") \
              .save()
              
      # hashtags
      hashtags = entities.filter(col("entities.hashtags").isNotNull()) \
                .select(explode(col("entities.hashtags"))) \
                .select("col.tag") \
                .groupBy("tag") \
                .count()
      hashtags \
              .write \
              .mode("append") \
              .format("jdbc") \
              .option("user","postgres") \
              .option("password","postgres") \
              .option("driver","org.postgresql.Driver") \
              .option("url","jdbc:postgresql://postgres:5432/postgres") \
              .option("dbtable","kafka_twitter_hashtags") \
              .save()
                
      # mentions
      mentions = entities.filter(col("entities.mentions").isNotNull()) \
                .select(explode(col("entities.mentions"))) \
                .select("col.username") \
                .groupBy("username") \
                .count()
      mentions \
              .write \
              .mode("append") \
              .format("jdbc") \
              .option("user","postgres") \
              .option("password","postgres") \
              .option("driver","org.postgresql.Driver") \
              .option("url","jdbc:postgresql://postgres:5432/postgres") \
              .option("dbtable","kafka_twitter_mentions") \
              .save()
            
kafka.writeStream \
  .option("checkpointLocation", "./checkpoint/") \
  .foreachBatch(batchFunction) \
  .start() \
  .awaitTermination()
     