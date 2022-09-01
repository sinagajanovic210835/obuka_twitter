import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType}

object SparkStreamDe extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val schema = StructType(
    Seq(
      StructField("attachments", StructType(
        Seq(
        StructField("media_keys", ArrayType(StringType, true), true),
        StructField("poll_ids", ArrayType(StringType, true), true)
      )), true),
      StructField("author_id", StringType, true),
      StructField("context_annotations", ArrayType(
        StructType(
          Seq(
            StructField("domain", StructType(
              Seq(
                StructField("description", StringType, true),
                StructField("id", StringType, true),
                StructField("name", StringType, true)
              )
            ), true),
            StructField("entity", StructType(
              Seq(
                StructField("description", StringType, true),
                StructField("id", StringType, true),
                StructField("name", StringType, true)
              )
            ), true)
          )
        )
      ), true),
      StructField("conversation_id", StringType, true),
      StructField("created_at", StringType, true),
      StructField("entities", StructType(Seq(
        StructField("annotations", ArrayType(
          StructType(
            Seq(
              StructField("end", LongType, true),
              StructField("normalized_text", StringType, true),
              StructField("probability", DoubleType, true),
              StructField("start", LongType, true),
              StructField("type", StringType, true)
            )
          )
        ), true),
        StructField("cashtags", ArrayType(
          StructType(
            Seq(
              StructField("end", LongType, true),
              StructField("start", LongType, true),
              StructField("tag", StringType, true)
            )
          )
        ), true),
        StructField("hashtags", ArrayType(
          StructType(
            Seq(
              StructField("end", LongType, true),
              StructField("start", LongType, true),
              StructField("tag", StringType, true)
            )
          )
        ), true),
        StructField("mentions", ArrayType(
          StructType(
            Seq(
              StructField("end", LongType, true),
              StructField("id", StringType, true),
              StructField("start", LongType, true),
              StructField("username", StringType, true)
            )
          )
        ), true),
        StructField("urls", ArrayType(
          StructType(Seq(
            StructField("description", StringType, true),
            StructField("display_url", StringType, true),
            StructField("end", LongType, true),
            StructField("expanded_url", StringType, true),
            StructField("images", ArrayType(
              StructType(Seq(
                StructField("height", LongType, true),
                StructField("url", StringType, true),
                StructField("width", LongType, true)
              ))
            ), true),
            StructField("media_key", StringType, true),
            StructField("start", LongType, true),
            StructField("status", LongType, true),
            StructField("title", StringType, true),
            StructField("unwound_url", StringType, true),
            StructField("url", StringType, true)
          ))
        ), true)
      )),true),
      StructField("geo", StructType(
        Seq(
          StructField("coordinates", StructType(
            Seq(
              StructField("coordinates", ArrayType(DoubleType), true),
              StructField("type", StringType, true)
            )
          ), true),
          StructField("place_id", StringType, true)
          )
      ), true),
      StructField("id", StringType, true),
      StructField("in_reply_to_user_id", StringType, true),
      StructField("lang", StringType, true),
      StructField("possibly_sensitive", BooleanType, true),
      StructField("public_metrics", StructType(
        Seq(
          StructField("like_count", LongType, true),
          StructField("quote_count", LongType, true),
          StructField("reply_count", LongType, true),
          StructField("retweet_count", LongType, true)
        )
      ), true),
      StructField("referenced_tweets", ArrayType(StructType(
        Seq(
          StructField("id", StringType, true),
          StructField("type", StringType, true)
        )
      )), true),
      StructField("reply_settings", StringType, true),
      StructField("source", StringType, true),
      StructField("text", StringType, true),
      StructField("wintheld", StructType(
        Seq(
          StructField("copyright", BooleanType, true),
          StructField("country_codes", ArrayType(StringType, true), true)
        )
      ), true)
    )
  )

  val spark = SparkSession
    .builder()
    .appName("ZadatakDE")
    .config("spark.master", "local")
    .getOrCreate()

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka1:19092")
    .option("subscribe", "twitter")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .load()

  /*
  "EndpointID": "9befd80833e0622cf30739d1d868192abf180dbb85050286b1e598e07b9e6a38",
                      "Gateway": "172.18.0.1",
                      "IPAddress": "172.18.0.10",
  */

  val db = Map(
    "user" -> "postgres", // database username
    "password" -> "postgres", // password
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://postgres:5432/postgres"
  )

  val kafka = df.selectExpr(
    "cast(value as string)"
  ).select(from_json(col("value").
    cast(StringType), schema).
    alias("value"))

  kafka.writeStream.foreachBatch (
    (batch: DataFrame, _: Long) => {

      //public_metrics
      /*val publicMetrics  = batch.select(
        col("value.id"),
        col("value.created_at"),
        col("value.text"),
        col("value.lang"),
        col("value.public_metrics.like_count"),
        col("value.public_metrics.quote_count"), //retweets with comments
        col("value.public_metrics.reply_count"),
        col("value.public_metrics.retweet_count")
      )
      val most_likes = publicMetrics.filter(col("like_count") >= 50)select(col("text"), col("like_count"))
      most_likes.write
        .format("jdbc")
        .options(db + ("dbtable" -> "kafka_likes"))
        .mode(SaveMode.Append)
        .save()
      */

      val geo = batch.filter(col("value.geo.coordinates").isNotNull).
        select(col("value.text"), col("value.geo")).
        select(col("text"), col("geo.coordinates.coordinates")).
        select(
          col("text"),
          col("coordinates").getItem(0).as("longitude"),
          col("coordinates").getItem(1).as("latitude")
        )
      geo.write
        .format("jdbc")
        .options(db + ("dbtable" -> "geo_kafka"))
        .mode(SaveMode.Append)
        .save()

      //geo location
 /*     val geo = batch.filter(col("value.geo.coordinates").isNotNull).
        select(col("value.text"), col("value.geo")).
        select(col("text"), col("geo.coordinates.coordinates")).
        select(
          col("text"),
          col("coordinates").getItem(0).as("longitude"),
          col("coordinates").getItem(1).as("latitude")
        )
      geo.write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "twitter4").
        format("console")
        .save()
 */
    }
     )
    .start()
    .awaitTermination()
}

