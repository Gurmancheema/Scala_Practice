/*Question: Given a large dataset of financial transactions, write a Spark program using Python or Scala that does the following:

Load the dataset from a CSV file into a Spark DataFrame.
Filter out transactions that have null or missing values in important columns, such as transaction_id, amount, or transaction_date.
Perform aggregation to calculate the total transaction amount and count of transactions for each user_id.
Write the output DataFrame containing user_id, total_amount, and transaction_count to a new CSV file in a specified output directory.
*/


// importing libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



//creating the entry point of the app
object inter_ques extends App {

//let build sparksession
val spark  = SparkSession.builder().appName("inter_ques").master("local[*]").getOrCreate()

// reading the dataframe into spark
val df = spark.read.option("header","true").csv("data/sample_financial_transactions.csv")

df.show()

val first_ans = df.select("*")
                   .where(
                    col("transaction_id").isNotNull &&
                   col("amount").isNotNull &&
                   col("transaction_date").isNotNull
                   )

first_ans.show()

val second_ans = first_ans.groupBy("user_id")
                    .agg(
                    count("transaction_id").alias("count_of_transactions"),
                    sum("amount").alias("total_transaction_amount")
                    )

second_ans.show()

//saving the output into new directory

second_ans.coalesce(1).write.option("header","true").csv("D:/scala_project/myfirstscalaproject/data/init_ans")

//stop the session
spark.stop()
}