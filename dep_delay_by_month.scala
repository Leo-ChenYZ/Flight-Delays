import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

val spark = SparkSession.builder().appName("Average Departure Delay by Month").getOrCreate()

val df = spark.read.option("header", "true").csv("project/flights_final.csv")

// Cast the 'dep_delay' column to Integer
val dfWithIntDepDelay = df.withColumn("dep_delay", col("dep_delay").cast(IntegerType))

// Group by month and calculate average departure delay
val avgDepDelayByMonth = dfWithIntDepDelay.groupBy("month")
  .agg(avg("dep_delay").alias("avg_dep_delay"))
  .orderBy(asc("month"))

// Show all months and their corresponding average delay length in chronological order
avgDepDelayByMonth.show()

// Show the top 3 months with the longest average delay times
avgDepDelayByMonth.orderBy(desc("avg_dep_delay")).limit(3).show()