import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkUsers extends App {

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

  val users = {
    spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .json("hdfs://namenode:9000/user/test/users")
  }

  users.printSchema()

  val metrics = users.select("username", "name", "public_metrics.followers_count", "public_metrics.following_count", "public_metrics.tweet_count")

  metrics.show()

  metrics.write.mode(SaveMode.Overwrite).
    format("jdbc").
    options(db + ("dbtable" -> "user_metrics")).
    save()

val media = {
      spark
        .read
        .option("inferSchema", "true")
        .option("header", "true")
        .json("hdfs://namenode:9000/user/test/media")
    }
  media.printSchema()

  val video = media.filter(!(col("type") === "photo") && col("public_metrics").isNotNull)

  video.select("variants.url").show(100, false)

  val video1 = video.select("preview_image_url", "public_metrics.view_count", "variants.url")

  video1.write.mode(SaveMode.Overwrite).
      format("jdbc").
      options(db + ("dbtable" -> "videos")).
      save()
}
