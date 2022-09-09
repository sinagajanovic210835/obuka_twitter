import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object SparkBatch extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val db = Map(
    "user" -> "postgres",
    "password" -> "postgres",
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://postgres:5432/postgres",
  )

  val spark =
    SparkSession
      .builder()
      .appName("TwitterDe")
      .master("local")
      .getOrCreate()

  val tweets = spark
              .read
              .option("inferSchema", "true")
              .option("header", "true")
              .json("hdfs://namenode:9000/user/test/clean_data")

  val annot = tweets.select(col("username"), explode(col("context_annotations"))).cache()

  //domains
  val domain = annot.select("col.domain.description", "col.domain.name").
    groupBy("description", "name").count()


    domain.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable"->"twitter_domains")).
      save()

    //entities
    val entity = annot.
      select("col.entity.name").
      groupBy("name").count()

    entity.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable"->"entities")).
      save()

    annot.unpersist()

    val entities = tweets.filter(col("entities").isNotNull).
      select("username", "entities").cache()

    //annotations
    val annotations = entities.filter(col("entities.annotations").isNotNull).
      select(explode(col("entities.annotations"))).
      select("col.type", "col.normalized_text").
      groupBy("type", "normalized_text").count()

    annotations.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable" -> "twitter_annotations")).
      save()

    //cashtags
    val cashtags = entities.filter(col("entities.cashtags").isNotNull).
      select(explode(col("entities.cashtags"))).
      select("col.tag").
      groupBy("tag").count()


    cashtags.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable" -> "twitter_cashtags")).
      save()

    //hashtags
    val hashtags = entities.filter(col("entities.hashtags").isNotNull).
      select(explode(col("entities.hashtags"))).
      select("col.tag").
      groupBy("tag").count()

    hashtags.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable" -> "twitter_hashtags")).
      save()


    //mentions
    val mentions = entities.filter(col("entities.mentions").isNotNull).
      select(explode(col("entities.mentions"))).
      select("col.username").
      groupBy("username").
      count()


    mentions.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable" -> "twitter_mentions")).
      save()

    entities.unpersist()

  //geo
    val geo = tweets.filter(col("geo.coordinates.coordinates").isNotNull).cache()
        val sol = geo.select(col("username"), col("text"),
        col("geo.coordinates.coordinates").getItem(0).alias("longitude"),
        col("geo.coordinates.coordinates").getItem(1).alias("latitude"))
          .distinct()

    sol.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable" -> "twitter_geo_locations")).
      save()

    val publicMetrics = tweets.filter(col("public_metrics").isNotNull).
      select("username", "text", "public_metrics").cache()

    //likes
    val likes = publicMetrics.
      select(col("username"), col("text"),
        col("public_metrics.like_count").alias("likes_count"),
        col("public_metrics.quote_count").alias("quotes_count"),
        col("public_metrics.reply_count").alias("replies_count"),
        col("public_metrics.retweet_count").alias("retweets_count"))

    likes.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable" -> "new_like_count")).
      save()

    //quotes
    val quotes = publicMetrics.
      filter(col("public_metrics.quote_count") > 0).
      select(col("username"), col("text"), col("public_metrics.quote_count").alias("quotes_count")).
      orderBy(col("quotes_count").desc)

    quotes.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable" -> "quote_count")).
      save()

    //replies
    val replies = publicMetrics.
      filter(col("public_metrics.reply_count") > 0).
      select(col("username"), col("text"), col("public_metrics.reply_count").alias("replies_count")).
      orderBy(col("replies_count").desc)

    replies.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable" -> "reply_count")).
      save()

    //retweets
    val retweets = publicMetrics.
      filter(col("public_metrics.retweet_count") > 0).
      select(col("username"), col("text"), col("public_metrics.retweet_count").alias("retweets_count")).
      orderBy(col("retweets_count").desc).
      distinct()

    retweets.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable" -> "retweet_count")).
      save()

    publicMetrics.unpersist()

    val referenced_tweets = tweets.filter(col("referenced_tweets").isNotNull).
      select(col("referenced_tweets.type")).
      groupBy("type").count()

    referenced_tweets.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable" -> "referenced_tweets_type")).
      save()

    //source
    val source = tweets.filter(col("source").isNotNull).
      select("source").
      groupBy("source").
      count()

    source.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable" -> "tweet_sources")).
      save()

    //lang
    val lang = tweets.filter(col("lang").isNotNull).
      select("lang").
      groupBy("lang").count()

    lang.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable" -> "tweet_languages")).
      save()

}
