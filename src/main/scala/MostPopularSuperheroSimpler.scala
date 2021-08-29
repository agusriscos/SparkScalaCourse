import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


/** Find the superhero with the most co-appearances. */
object MostPopularSuperheroSimpler {

  case class SuperHeroName(heroID: Int, heroName: String)
  case class Connection(heroID: Int, connections: Int)

  // Function to extract the hero ID and number of connections from each line
  def countCoOccurrences(line: String): Connection = {
    val elements = line.split("\\s+")
    Connection(elements(0).toInt, elements.length - 1)
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

    val superHeroNameSchema = new StructType()
      .add("heroID", IntegerType, nullable = true)
      .add("heroName", StringType, nullable = true)

    import spark.implicits._
    val names = spark
      .read
      .schema(superHeroNameSchema)
      .option("sep", " ")
      .csv("./data/Marvel-names.txt")
      .as[SuperHeroName]

    val lines = spark
      .sparkContext
      .textFile("./data/Marvel-graph.txt")

    val connections = lines.map(countCoOccurrences).toDS

    val connectionsCount = connections
      .groupBy("heroID")
      .sum("connections")
      .sort(desc("sum(connections)"))
    val mostPopularSuperhero = connectionsCount.first()

    val mostPopularName = names
      .filter($"heroID" === mostPopularSuperhero(0))
      .select("heroName")
      .first()

    println(s"$mostPopularName is the most popular superhero with ${mostPopularSuperhero(1)} co-appearances")
    spark.stop()

  }
  
}
