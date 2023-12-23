import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("Top 5 Carriers with Highest Probability of Delays").getOrCreate()

val df = spark.read.option("header", "true").csv("project/flights_final.csv")

// group the data by carrier, calculate the delay probability, and put in descending order
val carrierDelay = df.groupBy("carrier")
  .agg(avg("delay_status").alias("delay_probability"))
  .orderBy(desc("delay_probability"))
  .limit(5)

// print result
carrierDelay.show()