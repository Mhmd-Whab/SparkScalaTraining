package spark.training

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{pow, _}

import scala.io.{Codec, Source}
import math._

object Task2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("SecondTask")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("============================")
    println("============================")

    val jdbcDF1 = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "public.realestate")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    val jdbcDF2 = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "public.mock_data")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    jdbcDF1.printSchema()
    jdbcDF1.show(5)
    println("=======================")
    jdbcDF2.printSchema()
    jdbcDF2.show(5)

    println("DF2 group by country")

    val q = jdbcDF2.orderBy(asc("country"), asc("first_name"), asc("last_name"))
    q.show(q.count().toInt)
  }
}
