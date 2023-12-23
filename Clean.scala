import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.appName("Cleaning").getOrCreate()

var df = spark.read.option("header", "true").option("inferSchema", "true").csv("hw7/flights.csv")

// Drop columns I do not need
val columnsOfInterest = Array("month", "day", "dep_time", "dep_delay", "arr_time", "arr_delay", "carrier", "flight", "origin", "dest", "distance", "hour", "minute")
val columnsToDrop = df.columns.diff(columnsOfInterest)
df = df.drop(columnsToDrop: _*)
df.show()

// Write to HDFS
df.coalesce(1).write.option("header", "true").csv("flights_cleaned")

spark.stop()