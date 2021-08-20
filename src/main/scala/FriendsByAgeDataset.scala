import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FriendsByAgeDataset {

  case class Person(age:Int, numFriends:Int)

  def mapper(line:String): Person = {
    val fields = line.split(',')

    val person:Person = Person(fields(2).toInt, fields(3).toInt)
    person
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("FriendsByAgeDataset")
      .master("local[*]")
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._

    val lines = spark.sparkContext.textFile("./data/fakefriends.csv")
    val people = lines.map(mapper).toDS.cache()

    // Group by age and compute numFriends average
    val meanNumFriendsByAge = people
      .groupBy("age")
      .avg("numFriends")
      .withColumn("avg(numFriends)", col("avg(numFriends)").cast("int"))

    // Get and prettify results
    val resultDS = meanNumFriendsByAge
      .withColumnRenamed("avg(numFriends)", "avgNumFriends")
      .orderBy(col("avgNumFriends").desc)
    val results = resultDS.collect()

    people.unpersist()
    spark.stop()

    results.foreach(println)
  }
}