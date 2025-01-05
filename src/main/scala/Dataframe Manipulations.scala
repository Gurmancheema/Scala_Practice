// import the libraries
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

// Create the object or the entry point of this particualr script

object dataframe_manipulations extends App {

    // Creating a sparksession
    val spark = SparkSession.builder().master("local[*]").appName("dataframe_manipulations").getOrCreate()

    // From the previous lessons, let's import the same datafile & schema
    // as we are working further
   
    /* Entire Data Loading: The code will load the entire dataset into the DataFrame unless further transformations, actions, or filters are applied.
        Sampling for Schema Inference: The samplingRatio option only affects schema inference. 
        Spark will sample 0.1% of the rows to determine the structure of the data (e.g., column names and data types). 
        After schema inference, Spark processes the entire dataset.*/


    val df = spark
                    .read
                    .option("samplingRatio", 0.001)
                    .option("header", true)
                    .csv("data/sf-fire-calls.csv")

    // Let's start with data manipulation now

     // 1. Renaming an existing column in the dataframe  & retrieving some data

     val new_df = df.withColumnRenamed("Delay", "ResponseDelayedInMins")
                    .select("ResponseDelayedInMins")
                    .where(col("ResponseDelayedInMins") > 5)
                    .show(10)

    // 2. Converting the datatype of some date time columns from strings to datetime datatypes  & retrieving some data

    val changed_date = df.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
                    .drop("CallDate")
                    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
                    .drop("WatchDate")
                    .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm") , "MM/dd/yyyy"))
                    .drop("AvailableDtTm")  
    
        // how many years’ worth of Fire Department calls are included in the data set

    val fire_df_calls = df.select(year(col("IncidentDate")))
                                .distinct()
                                .orderBy(year(col("IncidentDate")))
                                .show()
    
    
    //3. what were the most common types of fire calls & give their count?

    val common_calls  = new_df.select("CallType")
                                .where(col("CallType").isNotNull)
                                .groupBy("CallType")
                                .count()
                                .orderBy(desc("count"))
                                .show(20,false)

    //4. let's compute the sum of alarms, the average response time, and the minimum 
     //   and maximum response times to all fire calls in our data set.

     val stats = new_df.select(sum("NumAlarms").alias("TotalAlarms")
                        ,avg("ResonseDelayedInMins").alias("AvgResponseTime")
                        ,min("ResponseDelayedInMins").alias("MinResponsetime")
                        ,max("ResponseDelayedInMins").alias("MaxResponsetime"))
                        .show()

    spark.stop()
}
