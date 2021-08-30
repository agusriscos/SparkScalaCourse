import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StringType, IntegerType}

/** Find the superhero with the most co-appearances. */
object MostPopularSuperheroDataset {
  
  case class SuperHeroName(heroID: Int, heroName: String)
  case class SuperHero(value: String)
 
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

    val superHeroNameSchema = new StructType()
      .add("heroID", IntegerType, nullable = true)
      .add("heroName", StringType, nullable = true)

    import spark.implicits._
    val names = spark
      .read
      .schema(superHeroNameSchema)
      .options(Map("sep" -> " ", "charset" -> "ISO-8859-1"))
      .csv("./data/Marvel-names.txt")
      .as[SuperHeroName]

    val lines = spark
      .read
      .text("./data/Marvel-graph.txt")
      .as[SuperHero]

    val connectionsCount = lines
      .withColumn("heroID", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .drop("value")
      .groupBy("heroID")
      .sum()
      .sort(desc("sum(connections)"))
    connectionsCount.show()

    val mostPopularSuperhero = connectionsCount.first()

    val mostPopularName = names
      .filter($"heroID" === mostPopularSuperhero(0))
      .select("heroName")
      .first()

    println(s"$mostPopularName is the most popular superhero with ${mostPopularSuperhero(1)} co-appearances")
    spark.stop()

  }
  
}
