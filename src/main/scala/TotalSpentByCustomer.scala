import org.apache.spark._
import org.apache.log4j.{Logger, Level}

object TotalSpentByCustomer {

  def parseLines(line: String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")

    // Do the magic
    val lines = sc.textFile("./data/customer-orders.csv")
    val fields = lines.map(parseLines)
    val countByCustomer = fields.reduceByKey(_ + _)

    // Bonus Track: Sort by value
    val countByCustomerSorted = countByCustomer.sortBy(_._2, ascending = false)

    // Get and print the results
    val results = countByCustomerSorted.collect()
    results.foreach(println)

  }

}
