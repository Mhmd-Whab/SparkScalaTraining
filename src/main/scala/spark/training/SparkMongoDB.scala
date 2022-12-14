package spark.training

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{pow, _}
import com.mongodb.spark._

object SparkMongoDB {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ReadMongoDB")
      .config("spark.mongodb.read.connection.uri", "mongodb://spark:spark@localhost:27017") // mongodb://user:pass@url:port
      .config("spark.mongodb.read.partitioner", "com.mongodb.spark.sql.connector.read.partitioner.PaginateBySizePartitioner")
      .config("spark.mongodb.read.partitioner.options.partition.field", "_id")
      .config("spark.mongodb.read.partitioner.options.partition.size", "4")
//      .config("spark.mongodb.read.partitioner.options.samples.per.partition", "13500")
      .config("spark.mongodb.write.connection.uri", "mongodb://spark:spark@localhost:27017")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    println("============================")
    println("============================")
    import spark.implicits._

    val df = spark.read
      .format("mongodb")
      .option("database", "mongospark")
      .option("collection", "iot")
      .load()

    df.printSchema()
    df.show(5)

    println("=========================")
    println("countries with temperature more than 25")
    println("=========================")

    println(df.rdd.getNumPartitions)

    val partitions = df
      .withColumn("partition_id", spark_partition_id())
      .groupBy("partition_id")
      .count()
      .orderBy(asc("partition_id"))
    partitions.show(partitions.count().toInt)

    val q = df
      .select("cn", "temp", "battery_level")
      .where(col("temp") > 25)
      .groupBy("cn")
      .agg(avg("temp").as("avg_temp")
        , avg("battery_level").as("avg_battery_level"))
      .orderBy(desc("avg_temp"))
      .withColumnRenamed("cn", "country")

    q.show()
    /*
    q
      .write
      .format("mongodb")
      .mode("Overwrite")
      .option("database", "mongospark")
      .option("collection", "hightemp_countries")
      .save()
    */

    println("## Jobs have finished -- Waiting for termination ##")
    val l = 1200 * math.pow(10, 3)
    Thread.sleep(l.toLong)
    spark.close()
  }
}
