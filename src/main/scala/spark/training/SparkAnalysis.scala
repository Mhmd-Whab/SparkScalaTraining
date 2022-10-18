package spark.training

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{pow, _}
import scala.io.{Codec, Source}
import math._

/* Use dataframes, sql to analyze data sets */

object SparkAnalysis {
  case class sf_fire(CallNumber: Int, UnitID: String, IncidentNumber: Int, CallType: String, CallDate: String
                     , WatchDate: String, CallFinalDisposition: String, AvailableDtTm: String
                     , Address: String, City: String, Zipcode: Int, Battalion: String
                     , StationArea: String, Box: String
                     , OriginalPriority: String, Priority: String, FinalPriority: Int
                     , ALSUnit: Boolean, CallTypeGroup: String
                     , NumAlarms: Int, UnitType: String, UnitSequenceInCallDispatch: Int
                     , FirePreventionDistrict: String
                     , SupervisorDistrict: String, Neighborhood: String, Location: String, RowID: String, Delay: Float)

  def start(): SparkSession ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("SparkAnalysis")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    println("==============================")
    println("==============================")

    return spark
  }
  def dataframeapi(file: DataFrame): Unit ={
    println("==================================")
    println("Starting dataframeapi function")
    println("==================================")
    file.printSchema()
    file.show(5)
    println("==================================")

    println("==================================")
    println("1. return the different types of fire calls in a given year, say 2018")
    println("==================================")

    val Q1 = file
      .select(col("CallType"))
      .distinct()
    Q1.show(Q1.count().toInt, false)

    println("==================================")
    println("2. What months within the year 2018 saw the highest number of fire calls")
    println("==================================")

    val Q2 = file
      .select( "CallDate" )
      .groupBy(month(col("CallDate")).as("month"))
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count", "fire_calls")
    Q2.show(Q2.count().toInt, false)

    println("==================================")
    println("3. Which neighborhood in San Francisco generated the most fire calls in 2018")
    println("==================================")

    val Q3 = file
      .select(col("Neighborhood"))
      .groupBy("Neighborhood")
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count", "fire_calls")
    Q3.show(Q3.count().toInt,false)

    println("==================================")
    println("4. Which neighborhoods had the worst response times to fire calls in 2018")
    println("==================================")

    val Q4 = file
      .select(col("Neighborhood"), col("Delay"))
      .groupBy(col("Neighborhood"))
      .agg(avg("Delay").as("avg_response_time"))
      .orderBy(desc("avg_response_time"))
    Q4.show(Q4.count().toInt, false)

    println("==================================")
    println("5. Which week in the year in 2018 had the most fire calls")
    println("==================================")

    val Q5 = file
      .select("CallDate")
      .groupBy(weekofyear(col("CallDate")).as("week"))
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count", "fire_calls")
    Q5.show(Q5.count().toInt, false)

    println("==================================")
    println("6. what is the most call type in a given year for each month")
    println("==================================")

    val Q6 = file
      .select(col("CallDate"), col("CallType"))
      .groupBy(month(to_date(col("CallDate"), "MM/dd/yyyy")).as("month"), col("CallType"))
      .count()
      .orderBy(asc("month"), desc("count"))
      .withColumnRenamed("count", "num_of_calls")
    Q6.show(Q6.count().toInt, false)

    println("==================================")
    println("7. what is the Calltype with highest delay each year")
    println("==================================")

    val Q7 = file
      .select("CallDate", "CallType", "Delay")
      .groupBy(col("CallType"))
      .agg(avg(col("Delay")).as("avg_delay"))
      .orderBy(desc("avg_delay"))
    Q7.show(Q7.count().toInt)
  }
  def sparksql(spark: SparkSession): Unit ={
    println("==================================")
    println("Starting sparksql function")
    println("==================================")
    val details = spark.sql("Describe sf_fire")
    details.show(details.count().toInt)
    spark.sql("select * from sf_fire limit 5").show()

    println("==================================")
    println("==================================")
    println("1. return the different types of fire calls in a given year, say 2018")
    println("==================================")

    val Q1 = spark.sql("select distinct CallType from sf_fire ")
    Q1.show(Q1.count().toInt, false)

    println("==================================")
    println("2. What months within the year 2018 saw the highest number of fire calls")
    println("==================================")

    val Q2 = spark.sql("select month(CallDate) as month , count(*) as fire_calls" +
      " from sf_fire " +
      "group by month" +
      " order by  fire_calls desc")
    Q2.show(Q2.count().toInt, false)

    println("==================================")
    println("3. Which neighborhood in San Francisco generated the most fire calls in 2018")
    println("==================================")

    val Q3 = spark.sql("select Neighborhood , count(*) as fire_calls " +
      "from sf_fire " +
      "group by Neighborhood " +
      "order by fire_calls desc")
    Q3.show(Q3.count().toInt, false)

    println("==================================")
    println("4. Which neighborhoods had the worst response times to fire calls in 2018")
    println("==================================")

    val Q4 = spark.sql("select Neighborhood, avg(Delay) as avg_delay " +
      "from sf_fire " +
      "group by Neighborhood " +
      "order by avg_delay desc")
    Q4.show(Q4.count().toInt, false)

    println("==================================")
    println("5. Which week in the year in 2018 had the most fire calls")
    println("==================================")

    val Q5 = spark.sql("select weekofyear(CallDate) as week , count(*) as fire_calls " +
      "from sf_fire " +
      "group by week " +
      "order by fire_calls desc ")
    Q5.show(Q5.count().toInt, false)

    println("==================================")
    println("6. what is the most call type in a given year for each month")
    println("==================================")

    val Q6 = spark.sql("select month(CallDate) as month, CallType, count(CallType) " +
      "as num_of_calls " +
      "from sf_fire " +
      "group by month(CallDate), CallType " +
      "order by month, num_of_calls desc")
    Q6.show(Q6.count().toInt, false)

    println("==================================")
    println("7. what is the Calltype with highest in the year")
    println("==================================")

    val Q7 = spark.sql("select CallType" +
      ", year(CallDate) as year " +
      ", AVG(Delay) as delay" +
      " from sf_fire" +
      " group by year, CallType " +
      "order by year desc, delay desc")
    Q7.show(Q7.count().toInt)
  }
  def datasetmethod(spark: SparkSession, file: Dataset[sf_fire]): Unit ={
    println("==================================")
    println("Starting datasetmethod function")
    println("==================================")
    file.printSchema()
    file.show(5)
    println("==================================")
    println("==================================")
    println("1. return the different types of fire calls in a given year, say 2018")
    println("==================================")

    val Q1 = file
      .select(col("CallType"))
      .distinct()
    Q1.show(Q1.count().toInt, false)

    println("==================================")
    println("2. What months within the year 2018 saw the highest number of fire calls")
    println("==================================")

    val Q2 = file
      .select("CallDate")
      .groupBy(month(col("CallDate")).as("month"))
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count", "fire_calls")
    Q2.show(Q2.count().toInt, false)

    println("==================================")
    println("3. Which neighborhood in San Francisco generated the most fire calls in 2018")
    println("==================================")

    val Q3 = file
      .select(col("Neighborhood"))
      .groupBy("Neighborhood")
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count", "fire_calls")
    Q3.show(Q3.count().toInt, false)

    println("==================================")
    println("4. Which neighborhoods had the worst response times to fire calls in 2018")
    println("==================================")

    val Q4 = file
      .select(col("Neighborhood"), col("Delay"))
      .groupBy(col("Neighborhood"))
      .agg(avg("Delay").as("avg_response_time"))
      .orderBy(desc("avg_response_time"))
    Q4.show(Q4.count().toInt, false)

    println("==================================")
    println("5. Which week in the year in 2018 had the most fire calls")
    println("==================================")

    val Q5 = file
      .select("CallDate")
      .groupBy(weekofyear(col("CallDate")).as("week"))
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count", "fire_calls")
    Q5.show(Q5.count().toInt, false)

    println("==================================")
    println("6. what is the most call type in a given year for each month")
    println("==================================")

    val Q6 = file
      .select(col("CallDate"), col("CallType"))
      .groupBy(month(to_date(col("CallDate"), "MM/dd/yyyy")).as("month"), col("CallType"))
      .count()
      .orderBy(asc("month"), desc("count"))
      .withColumnRenamed("count", "num_of_calls")
    Q6.show(Q6.count().toInt, false)

    println("==================================")
    println("7. what is the Calltype with highest in the year")
    println("==================================")

    val Q7 = file
      .select("CallDate", "CallType", "Delay")
      .groupBy(col("CallType"))
      .agg(avg(col("Delay")).as("avg_delay"))
      .orderBy(desc("avg_delay"))
    Q7.show(Q7.count().toInt)

  }
  def main(args: Array[String]): Unit = {

    val spark = start()

    val dataframe = spark.read
      .option("inferschema", true)
      .option("header", true)
      .csv("data/sf-fire-calls.csv")
      .withColumn("CallDate", to_date(col("CallDate"), "MM/dd/yyyy" ) )
      .where(year(col("CallDate")) === 2018)
    dataframe.cache()
    dataframe.count()

    val start_dataframeapi = System.nanoTime()
    dataframeapi(dataframe)
    val end_dataframeapi = System.nanoTime()

    dataframe.createOrReplaceTempView("sf_fire")

    val start_sparksql = System.nanoTime()
    sparksql(spark)
    val end_sparksql = System.nanoTime()

    val fire_schema = new StructType()
      .add("CallNumber", IntegerType, nullable = true)
      .add("UnitID", StringType, true)
      .add("IncidentNumber", IntegerType, true)
      .add("CallType", StringType, true)
      .add("CallDate", StringType, true)
      .add("WatchDate", StringType, true)
      .add("CallFinalDisposition", StringType, true)
      .add("AvailableDtTm", StringType, true)
      .add("Address", StringType, true)
      .add("City", StringType, true)
      .add("Zipcode", IntegerType, true)
      .add("Battalion", StringType, true)
      .add("StationArea", StringType, true)
      .add("Box", StringType, true)
      .add("OriginalPriority", StringType, true)
      .add("Priority", StringType, true)
      .add("FinalPriority", IntegerType, true)
      .add("ALSUnit", BooleanType, true)
      .add("CallTypeGroup", StringType, true)
      .add("NumAlarms", IntegerType, true)
      .add("UnitType", StringType, true)
      .add("UnitSequenceInCallDispatch", IntegerType, true)
      .add("FirePreventionDistrict", StringType, true)
      .add("SupervisorDistrict", StringType, true)
      .add("Neighborhood", StringType, true)
      .add("Location", StringType, true)
      .add("RowID", StringType, true)
      .add("Delay", FloatType, true)
    import spark.implicits._
    val dataset = spark.read
      .option("header", true)
      .schema(fire_schema)
      .csv("data/sf-fire-calls.csv")
      .withColumn("CallDate", to_date(col("CallDate"), "MM/dd/yyyy"))
      .where(year(col("CallDate")) === 2018)
      .as[sf_fire]
    dataset.cache()
    dataset.count()

    val start_dataset = System.nanoTime()
    datasetmethod(spark, dataset)
    val end_dataset = System.nanoTime()

    println("Execution time of dataframeapi is " +
      "" + ( end_dataframeapi- start_dataframeapi)/math.pow(10,9) + "seconds" )
    println("Execution time of sparksql is " + (end_sparksql - start_sparksql)/math.pow(10,9) + "seconds" )
    println("Execution time of dataset is " + (end_dataset - start_dataset)/math.pow(10,9) + "seconds" )

    spark.stop()
  }
}