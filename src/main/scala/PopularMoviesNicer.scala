import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}


/** Find the movies with the most ratings. */
object PopularMoviesNicer {

  case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec: Codec = Codec("ISO-8859-1") // This is the current encoding of u.item, not UTF-8.

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()
    val source = Source.fromFile("./data/ml-100k/u.item")
    val lines = source.getLines()
    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    source.close()
    movieNames
  }

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

    val nameDict = spark.sparkContext.broadcast(loadMovieNames())

    val ratingSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._
    val lines = spark.read
      .schema(ratingSchema)
      .option("sep", "\t")
      .csv("./data/ml-100k/u.data")
      .as[Movies]

    val movieCounts = lines
      .groupBy("movieID")
      .count()
      .sort(desc("count"))

    val lookupName: Int => String = (movieID: Int) => {
      nameDict.value(movieID)
    }
    val lookupNameUDF = udf(lookupName)

    val moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(col("movieID")))
    moviesWithNames.select("movieTitle", "count").show(moviesWithNames.count.toInt, truncate = false)

  }

}

