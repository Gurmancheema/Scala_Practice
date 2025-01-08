// importing the libraries
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

//creating an entry point for this script

object prac_ques_dataframe_api extends App{

    //initiating sparksession
    val spark = SparkSession.builder().master("local[*]").appName("prac_ques_dataframe_api").getOrCreate()

    //reading the datafile into a dataframe
    //we'll let spark infer the schema for this dataframe

    val df = spark.read
             .option("header",true)
             .option("samplingRatio",.001)
             .csv("data/sf-fire-calls.csv")

    df.show(5,false)
    // 1. • What were all the different types of fire calls in 2018?


    //firstly changing the datatype of CallDate column to date timestamp from string

     val new_df = df.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
                    .drop(col("CallDate"))
    
    //since we got a new dataframe with IncidentDate as new column having timestamp datatype
    //let's run our query on this dataframe now
    // to check the changed datatype ---> new_df.printSchema()
    val diff_fire_calls = new_df.select("CallType")
                            .where(col("CallType").isNotNull.and(year(col("IncidentDate")) === 2018))
                            .distinct()
                            .show()

    //2. What months within the year 2018 saw the highest number of fire calls?

    val high_fire_calls =  new_df.filter(col("CallType") === "Structure Fire").filter(year(col("IncidentDate")) === 2018)
                                .withColumn("Months", month(col("IncidentDate")))
                                .groupBy("Months")
                                .count()
                                .orderBy(desc("count"))
                                .show()

    //3. Which neighborhood in San Francisco generated the most fire calls in 2018?

    val neighbour_calls = new_df.filter(col("CallType") === "Structure Fire")
                                .filter(year(col("IncidentDate")) === 2018)
                                .groupBy("Neighborhood")
                                .count()
                                .orderBy(desc("count"))
                                .limit(1)
                                .show()

    //4. Which neighborhoods had the worst response times to fire calls in 2018?

    val wrst_res_time = new_df.filter(col("CallType") === "Structure Fire")
                            .filter(year(col("IncidentDate")) === 2018)
                            .groupBy("Neighborhood")
                            .agg(max("Delay").alias("ResponseTime"))
                            .orderBy(desc("ResponseTime"))

    //5. Which week in the year in 2018 had the most fire calls?

    val week_year_calls = new_df.filter(col("CallType") === "Structure Fire")
                                .filter(year(col("IncidentDate")) === 2018)
                                .withColumn("Weekofyear", weekofyear(col("IncidentDate")))
                                .groupBy("Weekofyear")
                                .count()
                                .orderBy(desc("count"))
                                .limit(1)
                                .show()

    //6. Is there a correlation between neighborhood, zip code, and number of fire calls?

    val corr_calls = new_df.select("Neighborhood", "Zipcode")
                            .groupBy("Neighborhood","Zipcode")
                            .count()
                            .orderBy(desc("count"))

    //7. How can we use Parquet files or SQL tables to store this data and read it back?
    // we can save any dataframe derived above as parquet file or sql table, for instance i'll store the
    // abve derived dataframe in parquet format
    
    corr_calls.write.parquet("data/week_year_calls")
        println("File saved in parquet format")

        //saving another dataframe as sql table
        wrst_res_time.write.saveAsTable("data/Worst_response_Time")
        println("File saved as SQLTable")
    




    spark.stop()
}
