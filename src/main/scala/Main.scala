import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ // Correct way to import


object Main extends App {
  /* creating sparksession*/
  val spark = SparkSession.builder().appName("spark-app").master("local[*]").getOrCreate()

  println("Hello, World!")

  val df = spark.read.option("header","true").csv("data/mnm_dataset.csv")
  df.show(10)

  val mnm_count = df.select("State","Color","Count")
                    .groupBy("State","Color")
                    .agg(count("Count").alias("Total"))
                    .orderBy(desc("Total"))
    
  mnm_count.show(20)
  //stop the spark sessiom
  spark.stop()
}