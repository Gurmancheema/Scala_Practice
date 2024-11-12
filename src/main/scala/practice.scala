import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ // Correct way to import

object Practice extends App {
// Initialize sparksession

val spark = SparkSession.builder()
            .appName("scala-prac")
            .master("local[*]")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
        
// transactions data 

val transactions = Seq(
  (1, 101, 500, "2024-10-15 08:00:00"),
  (2, 102, 300, "2024-10-10 09:00:00"),
  (3, 101, -200, "2024-10-20 08:30:00"),
  (4, 103, 1000, "2024-10-25 10:00:00"),
  (5, 101, 400, "2024-10-05 09:30:00"),
  (6, 102, -50, "2024-10-02 11:00:00")
)

// Create the dataframe

val df = spark.createDataFrame(transactions).toDF("transaction_id",
                                            "account_id",
                                            "transaction_amount",
                                            "transaction_date")

df.show()

val date_seperated = df.withColumn("transaction_date",to_date(col("transaction_date"),"yyyy-MM-dd"))

date_seperated.show()

val final_df = date_seperated.select("account_id","transaction_amount")
                            .groupBy("account_id")
                            .agg(sum("transaction_amount").alias("total_amount"))
                            .orderBy(desc("total_amount"))
final_df.show()
// stop the spark session

spark.stop()
}