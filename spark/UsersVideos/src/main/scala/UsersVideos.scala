import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object UsersVideos extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val spark = SparkSession
              .builder
              .appName("TwitterDe")
              .master("local")
              .getOrCreate()

  val users = spark
              .read
              .option("inferSchema", "true")
              .option("header", "true")
              .json("hdfs://namenode:9000/user/test/users")


  users.printSchema()

  val metrics = users.
    select("username","name", "profile_image_url", "public_metrics.followers_count", "public_metrics.following_count", "public_metrics.tweet_count")

  metrics.write.mode(SaveMode.Overwrite).
            format("jdbc").
            option("user", "postgres").
            option("password", "postgres").
            option("driver", "org.postgresql.Driver").
            option("dbtable", "user_metrics").
            option("url", "jdbc:postgresql://postgres:5432/postgres").
            save()

  val media = spark
              .read
              .option("inferSchema", "true")
              .option("header", "true")
              .json("hdfs://namenode:9000/user/test/media")

  media.printSchema()

  val video = media.filter(col("media.type") =!= "photo")
                    .filter(col("public_metrics").isNotNull())

  val video1 = video.select("preview_image_url", "public_metrics.view_count", "variants.url")

  video1.write.mode(SaveMode.Overwrite).
          format("jdbc").
          option("user", "postgres").
          option("password", "postgres").
          option("driver", "org.postgresql.Driver").
          option("dbtable", "videos").
          option("url", "jdbc:postgresql://postgres:5432/postgres").
          save()
}
