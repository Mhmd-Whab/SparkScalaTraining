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

    val spark = SparkSession
      .builder()
      .appName("SecondTask")
      .master("local[*]")
      .getOrCreate()

    val jdbcDF1 = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql:localhost/postgres")
      .option("dbtable", "public.realestate")
      .option("user", "postgres")
      .option("password", "Moha@010015")
      .load()

    jdbcDF1.show()

  }
}
