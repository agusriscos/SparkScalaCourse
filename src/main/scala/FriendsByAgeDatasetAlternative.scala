import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FriendsByAgeDatasetAlternative {

  case class Person(ID:Int, name:String, age:Int, numFriends:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("FriendsByAgeDatasetAlternative")
      .master("local[*]")
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.

    // You need a header in CSV file to run this
    import spark.implicits._
    val people = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("./data/fakefriends-with-header.csv")
      .as[Person]

    // Select columns and save DS to cache
    val fields = people.select("age", "numFriends")

    // Group by age and compute numFriends average
    fields
      .groupBy("age")
      .agg(round(avg("numFriends"), 2).alias("avgNumFriends"))
      .sort(desc("avgNumFriends"))
      .show()

  }
}