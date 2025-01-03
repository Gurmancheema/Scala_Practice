// importing the libraries
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


//creating the entry point for this particular scala script

object dataframe_prac extends App {

            //Creating a sparksession

            val spark =  SparkSession.builder.master("local[*]").appName("common_dataframe_expressions").getOrCreate

    //Now we want to read a csv datafile into a dataframe

            // 1. Define a schema
            // 2. Mention the datafile path
            // 3. Read the file into a dataframe

            val schema = StructType (Array(StructField("CallNumber", IntegerType, true),
                                            StructField("UnitID", StringType, true),
                                                StructField("IncidentNumber", IntegerType, true),
                                                StructField("CallType", StringType, true),
                                                StructField("CallDate", StringType, true),
                                                StructField("WatchDate", StringType, true),
                                                StructField("CallFinalDisposition", StringType, true),
                                                StructField("AvailableDtTm", StringType, true),
                                                StructField("Address", StringType, true),
                                                StructField("City", StringType, true),
                                                StructField("Zipcode", IntegerType, true),
                                                StructField("Battalion", StringType, true),
                                                StructField("StationArea", StringType, true),
                                                StructField("Box", StringType, true),
                                                StructField("OriginalPriority", StringType, true),
                                                StructField("Priority", StringType, true),
                                                StructField("FinalPriority", IntegerType, true),
                                                StructField("ALSUnit", BooleanType, true),
                                                StructField("CallTypeGroup", StringType, true),
                                                StructField("NumAlarms", IntegerType, true),
                                                StructField("UnitType", StringType, true),
                                                StructField("UnitSequenceInCallDispatch", IntegerType, true),
                                                StructField("FirePreventionDistrict", StringType, true),
                                                StructField("SupervisorDistrict", StringType, true),
                                                StructField("Neighborhood", StringType, true),
                                                StructField("Location", StringType, true),
                                                StructField("RowID", StringType, true),
                                                StructField("Delay", FloatType, true))
                                    )


            val filepath = "data/sf-fire-calls.csv"

            val df = spark.read.schema(schema).option("header","true").csv(filepath)

            //df.show(5,false) //display the created dataframe

    //Performing data manipulation using filters & select statements just like in SQL

        // 1. Let's retrieve the rows where there was no "medical incident" & return only "IncidentNumber", "AvailableDtTm & "CallType"

        // Thing to learn is that Spark will create a new dataframe for this query's result as dataframes are immutable in Spark

        val query1_df = df.select("IncidentNumber", "AvailableDtTm", "CallType")
                            .where(col("CallType") =!= "Medical Incident")

            query1_df.show(5)   //keep in mind to comment the earlier show function to increase efficiency

        // 2. What if we want to know how many distinct CallTypes were recorded as the causes of the fire calls? 

        val query2_df = df.select("CallType")
                            .where(col("CallType").isNotNull)
                            .agg(countDistinct("CallType") as ("DistinctCallTypes"))
                            .show()

        

        // 3. What if we want to LIST the distinct CallTypes which were recorded as the causes of the fire calls

        val query3_df = df.select("CallType")
                            .where(col("CallType").isNotNull)
                            .distinct()
                            .show(10)










spark.stop()

}
