import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, IntegerType, StringType}

/** Find the movies with the most ratings. */
object PopularMovies {

  case class Rating(movieID: Int)
  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
     val spark = SparkSession
       .builder
       .appName("PopularMovies")
       .master("local[*]")
       .getOrCreate()

    val ratingSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", IntegerType, nullable = true)

    import spark.implicits._
    val lines = spark.read
      .schema(ratingSchema)
      .option("sep", "\t")
      .csv("./data/ml-100k/u.data")
      .as[Rating]

    lines
      .groupBy("movieID")
      .count()
      .sort(desc("count"))
      .show()

  }
  
}

