import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

val spark = SparkSession.builder()
  .appName("Rank NYC Airports by Departure Delay").getOrCreate()

val df = spark.read.option("header", "true").csv("project/flights_final.csv")

// Cast the 'dep_delay' column to Integer
val dfWithIntDepDelay = df.withColumn("dep_delay", col("dep_delay").cast(IntegerType))

// Group by origin airport and calculate the average departure delay (in minutes)
val avgDepDelayByOrigin = dfWithIntDepDelay.groupBy("origin")
  .agg(avg("dep_delay").alias("avg_dep_delay"))
  .orderBy(asc("avg_dep_delay")) // ascending order

avgDepDelayByOrigin.show()