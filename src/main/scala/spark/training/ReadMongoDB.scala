package spark.training

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{pow, _}
import com.mongodb.spark._

object ReadMongoDB {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ReadMongoDB")
      .config("spark.mongodb.read.connection.uri", "mongodb://spark:spark@localhost:27017") // mongodb://user:pass@url:port
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    println("============================")
    println("============================")

    import spark.implicits._
    val df = spark.read
      .format("mongodb")
      .option("database", "mongospark")
      .option("collection", "sample")
      .load()

    df.printSchema()
    df.show()
  }
}
