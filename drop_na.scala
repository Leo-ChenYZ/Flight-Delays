import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("Drop NA").getOrCreate()

val df = spark.read.option("header", "true").csv("hw8/flights_cleaned_hw8/flights_cleaned_hw8.csv")
	.toDF("month", "day", "dep_time", "dep_delay", "arr_time", "arr_delay", "carrier", "flight", "origin", "dest", "distance", "hour", "minute", "date", "delay_status")

// drop rows with NA values - missing values are coded with text value "NA"

// define a function to check if a column contains "NA"
def containsNA(column: String) = col(column) =!= "NA"
// apply the function to each column
val condition = df.columns.map(containsNA).reduce(_ && _)
// filter df to remove rows with "NA" in any column
val cleanedDF = df.filter(condition)

cleanedDF.show(10)
cleanedDF.coalesce(1).write.option("header", "true").csv("project")