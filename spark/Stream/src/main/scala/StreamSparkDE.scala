import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.DriverManager

object  StreamSparkDE extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

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
      )), true),
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
      StructField("last_modified", StringType, true),
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
      ), true),
      StructField("username", StringType, true)
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
    .option("subscribe", "twitter1")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "latest")
    .load()

  val db = Map(
    "user" -> "postgres",
    "password" -> "postgres",
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://postgres:5432/postgres"
  )

//  val connection = DriverManager.getConnection(db("url")+"?"+"user="+db("user")+"&"+"password="+db("password"))
//
//  val st = connection.prepareStatement(
//    "START TRANSACTION; " +
//    "DROP TABLE IF EXISTS geo_kafka;" +
//    "DROP TABLE IF EXISTS kafka_quote_count;" +
//    "DROP TABLE IF EXISTS kafka_reply_count;" +
//    "DROP TABLE IF EXISTS kafka_twitter_annotations;" +
//    "DROP TABLE IF EXISTS kafka_twitter_cashtags;" +
//    "DROP TABLE IF EXISTS kafka_twitter_mentions;" +
//    "DROP TABLE IF EXISTS kafka_twitter_hashtags;" +
//    "END TRANSACTION;"
//  )
//
//  st.execute()
//
//  connection.close()

  val kafka = df.selectExpr(
           "cast (key as string)",
                  "cast (value as string)"
  ).select(col("key"), from_json(col("value")
    .cast(StringType), schema)
    .alias("value"))


  kafka.writeStream
  .option("checkpointLocation", "/checkpoint/")
  .foreachBatch(
    (batch: DataFrame, _:Long) => {

      val geo = batch.filter(col("value.geo.coordinates").isNotNull)
        .select(col("value.author_id"),
          col("value.text"),
          col("value.geo.coordinates.coordinates").getItem(0).as("longitude"),
          col("value.geo.coordinates.coordinates").getItem(1).as("latitude")
        )

      geo.write
        .format("jdbc")
        .options(db + ("dbtable" -> "geo_kafka"))
        .mode(SaveMode.Append)
        .save()

//      val publicMetrics = batch.filter(col("value.public_metrics").isNotNull).
//            select(col("value.username").alias("username"),
//              col("value.text").alias("text"),
//              col("value.public_metrics").alias("public_metrics"))
//
//          //quotes
//          val quotes = publicMetrics.
//            filter(col("public_metrics.quote_count") > 0).
//            select(col("username"),
//              col("text"),
//              col("public_metrics.quote_count").alias("quotes_count")
//            )
//
//          quotes.write.mode(SaveMode.Append).
//            format("jdbc").
//            options(db + ("dbtable" -> "kafka_quote_count")).
//            save()
//
//          //replies
//          val replies = publicMetrics.
//            filter(col("public_metrics.reply_count") > 0).
//            select(col("username"),
//              col("text"),
//              col("public_metrics.reply_count").alias("replies_count")
//            )
//
//          replies.write.mode(SaveMode.Append).
//            format("jdbc").
//            options(db + ("dbtable" -> "kafka_reply_count")).
//            save()
//
          val entities = batch.filter(col("value.entities").isNotNull)
            .select(col("value.username").alias("username"),
              col("value.entities").alias("entities"))
      //
          //annotations
          val annotations = entities.filter(col("entities.annotations").isNotNull).
            select(explode(col("entities.annotations"))).
            select("col.type", "col.normalized_text").
            groupBy("type", "normalized_text").count()

          annotations.write.mode(SaveMode.Append).
            format("jdbc").
            options(db + ("dbtable" -> "kafka_twitter_annotations")).
            save()

          //cashtags
          val cashtags = entities.filter(col("entities.cashtags").isNotNull).
            select(explode(col("entities.cashtags"))).
            select("col.tag").
            groupBy("tag").count()


          cashtags.write.mode(SaveMode.Append).
            format("jdbc").
            options(db + ("dbtable" -> "kafka_twitter_cashtags")).
            save()


          //hashtags
          val hashtags = entities.filter(col("entities.hashtags").isNotNull).
            select(explode(col("entities.hashtags"))).
            select("col.tag").
            groupBy("tag").count()

          hashtags.write.mode(SaveMode.Append).
            format("jdbc").
            options(db + ("dbtable" -> "kafka_twitter_hashtags")).
            save()


          //mentions
          val mentions = entities.filter(col("entities.mentions").isNotNull).
            select(explode(col("entities.mentions"))).
            select("col.username").
            groupBy("username").
            count()

          mentions.write.mode(SaveMode.Append).
            format("jdbc").
            options(db + ("dbtable" -> "kafka_twitter_mentions")).
            save()
    }
  )
    .start()
    .awaitTermination()
}

//val deSer = new StringDeserializer
//val ser = new StringSerializer
//val props = {
//  val p = new Properties()
//  p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9092")
//  p.put(ConsumerConfig.GROUP_ID_CONFIG, "test")
//  p
//}
//
//val propsProd = {
//  val p = new Properties()
//  p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9092")
//  p
//}
//
//val consumer = new KafkaConsumer(props, deSer, deSer)
//val producer = new KafkaProducer(propsProd, ser, ser)
//
//consumer.subscribe(Collections.singleton("topic2"))
//
//try {
//  while (true) {
//    val records = consumer.poll(Duration.ofMillis(100))
//
//    records.forEach(rec => {
//      producer.send(new ProducerRecord[String, String]("topic3", "newTopic", rec.timestamp() + ">>" + rec.value().replaceAll(",", "   ")), new Callback {
//        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = if (exception != null) exception.printStackTrace()
//      })
//      println(rec.value())
//    })
//  }
//} catch {
//  case e: Exception =>
//    println(">>>>>>>>>>>>>>>>>>>>>  NULL   <<<<<<<<<<<<<<<<<<<")
//    e.printStackTrace()
//} finally {
//  consumer.close()
//}