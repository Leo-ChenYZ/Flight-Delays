import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

val spark: SparkSession = SparkSession.builder.appName("analysis").getOrCreate()

val flights: DataFrame = spark.read.option("header", "true").csv("project/flights_final.csv")
val numericColumns = Seq("dep_delay", "arr_delay", "distance", "hour", "minute")

val flightsConverted: DataFrame = numericColumns.foldLeft(flights) { (df, colName) =>
  df.withColumn(colName, col(colName).cast(IntegerType))
}

// Calculate mean, median, and mode for each column
for (column <- numericColumns) {
  println(s"Column: $column")

  // Mean
  val meanValue = flightsConverted.select(avg(column)).head().getDouble(0)
  println(s"Mean: $meanValue")

  // Median
  val medianValue = flightsConverted.stat.approxQuantile(column, Array(0.5), 0.25)(0)
  println(s"Median: $medianValue")

  // Mode
  val modeValue = flightsConverted.groupBy(column).count().sort(desc("count")).head().get(0)
  println(s"Mode: $modeValue")

  println("=========================")
}

// Standard deviation for "arr_delay"
val stdDevValue = flightsConverted.select(stddev("arr_delay")).head().getDouble(0)
println(s"Standard Deviation of arr_delay: $stdDevValue")