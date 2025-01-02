/* importing libraries*/
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._ // Correct way to import



object prac_from_book extends App
{
    //creating a sparksession
    val spark = SparkSession.builder.
                appName("reading_data")
                .master("local[*]")
                .getOrCreate()
            
    //reading a file
    //So first defing the schema as the file is in JSON format

    //Schema definition

    val schema = StructType(Array (StructField("Id",IntegerType,false),
                                    StructField("First",StringType,false),
                                    StructField("Last",StringType,false),
                                    StructField("Url",StringType,false),
                                    StructField("Published",StringType,false),
                                    StructField("Hits",IntegerType,false),
                                    StructField("Campaigns",ArrayType(StringType),false)
    ))

    //After making the schema, let's give the file path to a variable

    val filepath = "data/blogs.json"

    //reading the file into a dataframe

    val df= spark.read.schema(schema).json(filepath)
    df.show()           
        
    spark.stop()
            
            
            
            
            
            
            
}
