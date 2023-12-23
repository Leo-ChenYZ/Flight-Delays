import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.appName("Count Records").getOrCreate()
import spark.implicits._

val df = spark.read.option("header", "true").csv("hw7/flights.csv")

// Count number of records
val recordCount = df.count()
println(s"Total number of records: $recordCount")

// Explore dataset and schema:
println("Explore dataset and schema: ")
df.printSchema()

// Map and count based on carrier
println("Map and count based on carrier: ")
val carrierCounts = df.rdd
  .map(row => (row.getAs[String]("carrier"), 1))
  .reduceByKey(_ + _)
carrierCounts.collect().foreach(println)

println("Distinct values: ")
val columnsOfInterest = Seq("month", "day", "dep_time", "dep_delay", "arr_time", "arr_delay", "carrier", "flight", "origin", "dest", "distance", "hour", "minute")
val limit = 10
for (columnName <- columnsOfInterest) {
  val distinctValues = df.select(columnName).distinct().limit(limit)
  println(s"Column: $columnName")
  distinctValues.show()
}

println("Count distinct values: ")
// Let me try another syntax
columnsOfInterest.foreach {columnName =>
  val distinctCount = df.select(columnName).distinct().count()
  println(s"Count of distinct values in $columnName: $distinctCount")
}