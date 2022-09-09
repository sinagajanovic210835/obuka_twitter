import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object CleanData extends App {

  //  Clean Batch Data - remove duplicates an add username and name

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
      .json("hdfs://namenode:9000/user/test/splits")


  val users =
    spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .json("hdfs://namenode:9000/user/test/users")
      .select(col("id").alias("uid"), col("username"))

  val temp = tweets.select(col("id").cast(LongType).alias("id1"),
     col("last_modified").cast(LongType))
    .groupBy("id1")
    .agg(max(col("last_modified")).alias("l_modified"))

  val clean_data = tweets
    .join(temp, col("id") === col("id1") && col("last_modified") === col("l_modified"), "inner")
    .drop("id1", "l_modified")

  val tweetsWithUsername = clean_data.join(users, col("author_id") === col("uid"))
    .drop("uid")

  tweetsWithUsername
    .repartition(6)
    .write
    .json("hdfs://namenode:9000/user/test/clean_data")
}
