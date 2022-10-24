package spark.training

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import math._

// Analyze movies ratings data from 1995 to 2019, 25M records
object BigDataPostgres {

  def start(): SparkSession ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkconf = new SparkConf()
//      .set("spark.dynamicAllocation.enabled", "true")
//      .set("spark.dynamicAllocation.minExecutors", "2")
//      .set("spark.dynamicAllocation.schedulerBacklogTimeout", "1m")
//      .set("spark.dynamicAllocation.maxExecutors", "6")
//      .set("spark.dynamicAllocation.executorIdleTimeout", "2min")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "1g")
//      .set("spark.executor.cores" , "8") // There is only 1 executor locally
      .set("spark.sql.shuffle.partitions", "200")

    val spark = SparkSession
      .builder()
      .appName("BigData")
      .config(sparkconf)
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("============================")
    println("============================")

//    // Get conf
//    val mconf = spark.conf.getAll
//    // Print them
//    for (k <- mconf.keySet) {
//      println(s"${k} -> ${mconf(k)}\n")
//    }
    return spark
  }
  def read_postgres(spark:SparkSession, table: String): DataFrame ={
    val dbtable = "public." + table
    val numPartitions = 1

    val startT = System.nanoTime()
    val df = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
//      .option("dbtable", s"(SELECT MOD(ABS(row_number() over ()), $numPartitions) AS partitioncol" +
//        s", * from $dbtable) AS t")
      .option("dbtable", dbtable)
      .option("user", "postgres")
      .option("password", "spark")
//      .option("partitionColumn", "partitioncol")
//      .option("lowerBound", 0)
//      .option("upperBound", numPartitions)
//      .option("numPartitions",numPartitions)
      .load()
    val endT = System.nanoTime()
    println("partitions = " + df.rdd.getNumPartitions)
    println("It took " + ((endT - startT) / math.pow(10, 9)) + " seconds to read the file")

    df.printSchema()
    return df
  }
  def main(args: Array[String]): Unit = {
    // Start the SparkSession
    val spark = start()
    var startT: Long = 0
    var endT: Long = 0
    import spark.implicits._

    // Read the ratings table
    val ratings = read_postgres(spark, "ratings")
    // Read the movies table
    val movies = read_postgres(spark, "movies")

    // Get the number of records in each of the 16 partition

    var partitions = ratings
      .withColumn("partition_id", spark_partition_id())
      .groupBy("partition_id")
      .count()
      .orderBy(asc("partition_id"))
    partitions.show(partitions.count().toInt)

    partitions = movies
      .withColumn("partition_id", spark_partition_id())
      .groupBy("partition_id")
      .count()
      .orderBy(asc("partition_id"))
    partitions.show(partitions.count().toInt)


    // Join the 2 tables on movieID
    val mydf = ratings
      .join(functions.broadcast(movies), ratings("movieid") === movies("mid"))
      .withColumn("time", to_timestamp(from_unixtime(col("timestamp")), "yyyy-MM-dd HH:mm:ss" ))
      .select("userid", "movieid", "movie", "type", "rating", "time")
    mydf.printSchema()
//    mydf.cache()
//    mydf.count()

    // Query 1: count the number of votes for each rating and write it into the countRatings table

    startT = System.nanoTime()
    val q1 = mydf
      .groupBy("rating")
      .count()
      .orderBy(desc("rating"))
    q1.show()
    endT = System.nanoTime()
    println("It took " + ((endT-startT)/math.pow(10,9)) + " seconds to get the results")
    /*
    startT = System.nanoTime()
    q1
      .write
      .format("jdbc")
      .mode("Overwrite")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "public.countRatings")
      .option("user", "postgres")
      .option("password", "spark")
      .save()
    endT = System.nanoTime()
    println("It took " + ((endT - startT) / math.pow(10, 9)) + " seconds to write the results to the table")
    */

    // Query 2: count the number of votes for each type and write the results to the countType table

    startT = System.nanoTime()
    val q2 = mydf
      .groupBy("type")
      .count()
      .orderBy(desc("count"))
    q2.show(10)
    endT = System.nanoTime()
    println("It took " + ((endT - startT) / math.pow(10, 9)) + " seconds to get the results")
    /*
    startT = System.nanoTime()
    q2
      .write
      .format("jdbc")
      .mode("Overwrite")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "public.countType")
      .option("user", "postgres")
      .option("password", "spark")
      .save()
    endT = System.nanoTime()
    println("It took " + ((endT - startT) / math.pow(10, 9)) + " seconds to write the results to the table")
    */

    // Query 3: get the average rating of comedian movies from 2011 to 2019

    startT = System.nanoTime()
    val q3 = mydf
      .select("rating", "type", "time")
      .where( year(col("time")) >= 2011 && col("type").contains("Comedy"))
      .groupBy(col("type"), year(col("time")).as("year"))
      .agg(avg("rating").as("avg_rating"), count(lit(1)).alias("numberofratings"))
      .orderBy(desc("year"), desc("avg_rating"))
    q3.show(10)
    endT = System.nanoTime()
    println("It took " + ((endT - startT) / math.pow(10, 9)) + " seconds to get the results")


    println("## Jobs have finished -- Waiting for termination ##")
    val l = 1200 * math.pow(10,3)
    Thread.sleep(l.toLong)
    spark.close()
  }
}

//java.lang.OutOfMemoryError: GC overhead limit exceeded
//22/10/21 02:12:03 ERROR Executor: Exception in task 0.0 in stage 26.0 (TID 64)
//org.postgresql.util.PSQLException: Ran out of memory retrieving query results.