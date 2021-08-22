import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, IntegerType, StringType}

object FriendsByAgeDataset {

  case class Person(id:Int, name: String, age:Int, numFriends:Int)
  
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
    val personSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = true)
      .add("age", IntegerType, nullable = true)
      .add("numFriends", IntegerType, nullable = true)

    import spark.implicits._
    val people = spark.read
      .schema(personSchema)
      .csv("./data/fakefriends.csv")
      .as[Person]

    val ageAndFriends = people.select("age", "numFriends")

    // Group by age and compute numFriends average
    val meanNumFriendsByAge = ageAndFriends
      .groupBy("age")
      .avg("numFriends")
      .withColumn("avgNumFriends", round($"avg(numFriends)", 2))
      .select("age", "avgNumFriends")

    // Prettify and show results
    meanNumFriendsByAge
      .sort(desc("avgNumFriends"))
      .show(meanNumFriendsByAge.count.toInt)

  }
}