import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSortedDataset {

  // Case class book to load text file with DataSets (not recommended)
  case class Book(value: String)

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    // First. Load and split every line into words... Here is better to use RDDs as we work with
    // unstructured data (text)

    import spark.implicits._
    val input = spark.sparkContext.textFile("./data/book.txt")
    val words = input.flatMap(_.split("\\W+")).toDS
    val lowercaseWords = words.select(lower($"value").alias("word"))

    /*
    // This is the way to do it with DataSets only!
    val input = spark.read.text("./data/book.txt").as[Book]
    val words = input
      .select(explode(split($"value", "\\W+")).alias("word"))
      .filter($"word" =!= "")

    // Normalize everything to lowercase
    val lowercaseWords = words
      .select(lower($"word").alias("word"))
    */

    // Count of the occurrences of each word and sort by column in desc order
    val wordCount = lowercaseWords
      .groupBy("word").count()
      .sort(desc("count"))

    // To show all rows of the DS
    wordCount.show(wordCount.count.toInt)

  }
}

