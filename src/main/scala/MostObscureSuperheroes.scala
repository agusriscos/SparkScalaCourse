import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


/** Find the superhero with the most co-appearances. */
object MostObscureSuperheroes {

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

    val minConnection = connectionsCount
      .agg(min("sum(connections)"))
      .first()
      .getLong(0)

    val obscureSuperheroes = connectionsCount
      .filter($"sum(connections)" === minConnection)

    val result = obscureSuperheroes
      .join(names, usingColumn = "heroID")
      .select("heroName")

    println(s"Heroes with $minConnection connections:")
    result.show(result.count.toInt)
    spark.stop()

  }
  
}
