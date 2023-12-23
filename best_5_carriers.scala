import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("Top 5 Carriers with Lowest Probability of Delays").getOrCreate()

val df = spark.read.option("header", "true").csv("project/flights_final.csv")

val carrierDelay = df.groupBy("carrier")
  .agg(avg("delay_status").alias("delay_probability"))
  .orderBy(asc("delay_probability"))
  .limit(5)

carrierDelay.show()