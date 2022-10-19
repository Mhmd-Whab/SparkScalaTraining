package spark.training

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import math._
object BigDataPostgres {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("BigData")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("============================")
    println("============================")

    var startT = System.nanoTime()
    val ratings = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "public.ratings")
      .option("user", "postgres")
      .option("password", "spark")
      .load()
    var endT = System.nanoTime()
    println("It took " + ((endT-startT)/math.pow(10,9)) + " seconds to read the file")
    ratings.printSchema()

    startT = System.nanoTime()
    val movies = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "public.movies")
      .option("user", "postgres")
      .option("password", "spark")
      .load()
    endT = System.nanoTime()
    println("It took " + ((endT - startT) / math.pow(10, 9)) + " seconds to read the file")
    movies.printSchema()

    val mydf = ratings
      .join(movies, ratings("movieid") === movies("mid"))
      .withColumn("time", to_timestamp(from_unixtime(col("timestamp")), "yyyy-MM-dd HH:mm:ss" ))
      .select("userid", "movieid", "movie", "type", "rating", "time")

    mydf.printSchema()
    /*
    startT = System.nanoTime()
    val q1 = jdbcDF1
      .groupBy("rating")
      .count()
      .orderBy(desc("rating"))
    q1.show()
    endT = System.nanoTime()
    println("It took " + ((endT-startT)/math.pow(10,9)) + " seconds to get the results")
    */

    startT = System.nanoTime()
    val q2 = mydf
      .groupBy("type")
      .count()
      .orderBy(desc("count"))
    q2.show(10)
    endT = System.nanoTime()
    println("It took " + ((endT - startT) / math.pow(10, 9)) + " seconds to get the results")
  }
}
