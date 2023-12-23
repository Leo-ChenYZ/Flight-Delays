import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("Drop NA").getOrCreate()

val df = spark.read.option("header", "true").csv("hw8/flights_cleaned_hw8/flights_cleaned_hw8.csv")

// Drop rows with NA values
val cleanedDF = df.na.drop()
cleanedDF.show(10)
cleanedDF.coalesce(1).write.option("header", "true").csv("hw9/flights_cleaned_hw9")