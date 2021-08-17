import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
    
object DataFrames {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    person
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("DataFrames")
      .master("local[*]")
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    val lines = spark.sparkContext.textFile("./data/fakefriends.csv")
    val people = lines.map(mapper).toDS
    val peopleCached = people.select("name", "age").cache()
    
    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")
    
    println("Here is our inferred schema:")
    peopleCached.printSchema()
    
    println("Let's select the name column:")
    peopleCached.select("name").show()
    
    println("Filter out anyone over 21:")
    peopleCached.filter(peopleCached("age") < 21).show()
   
    println("Group by age:")
    peopleCached.groupBy("age").count().show()
    
    println("Make everyone 10 years older:")
    peopleCached.select(peopleCached("name"), peopleCached("age") + 10).show()

    peopleCached.unpersist()
    spark.stop()
  }
}