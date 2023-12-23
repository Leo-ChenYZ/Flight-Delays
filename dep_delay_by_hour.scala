import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

val spark = SparkSession.builder().appName("Average Departure Delay by Hour").getOrCreate()

val df = spark.read.option("header", "true").csv("project/flights_final.csv")

// Cast the 'dep_delay' column to Integer
val dfWithIntDepDelay = df.withColumn("dep_delay", col("dep_delay").cast(IntegerType))

// Group by hour and calculate average departure delay
val avgDepDelayByHour = dfWithIntDepDelay.groupBy("hour")
  .agg(avg("dep_delay").alias("avg_dep_delay"))
  .orderBy(asc("hour"))

// Show all hours and their corresponding average delay length in chronological order
avgDepDelayByHour.show()

// Show the top 3 hours with the longest average delay times
avgDepDelayByHour.orderBy(desc("avg_dep_delay")).limit(3).show()