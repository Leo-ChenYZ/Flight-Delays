import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

val spark: SparkSession = SparkSession.builder.appName("cleaning").getOrCreate()

val flights: DataFrame = spark.read.option("header", "true").csv("hw7/flights_cleaned/flights_cleaned.csv")

// 1. Date formatting: create a new column for the corresponding date based on "month" and "day"
val flightsWithDate: DataFrame = flights.withColumn("date",
  concat(col("month"), lit("/"), col("day")))

// 2. Create a binary column based on the condition of another column: delay status (0: no delay, 1: delay)
val flightsWithDelayStatus: DataFrame = flightsWithDate.withColumn("delay_status",
  when(col("arr_delay").cast(IntegerType) <= 0, 0).otherwise(1)
)

flightsWithDelayStatus.show(10)
flightsWithDelayStatus.coalesce(1).write.option("header", "true").csv("hw8/flights_cleaned_hw8")