import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, IntegerType, FloatType}
import org.apache.spark.sql.functions._

object TotalSpentByCustomerDataset {

  case class Order(customerID: Int, productID: Int, money_spent: Float)

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("TotalSpentByCustomer")
      .master("local[*]")
      .getOrCreate()

    // Do the magic
    val orderSchema = new StructType()
      .add("customerID", IntegerType, nullable = false)
      .add("productID", IntegerType, nullable = false)
      .add("money_spent", FloatType, nullable = true)

    import spark.implicits._
    val orders = spark.read
      .schema(orderSchema)
      .csv("./data/customer-orders.csv")
      .as[Order]

    val fields = orders.select("customerID", "money_spent")
    val sumByCustomer = fields
      .groupBy("customerID")
      .agg(round(sum("money_spent"), 2).alias("total_spent"))
      .sort("total_spent")

    sumByCustomer.show(sumByCustomer.count.toInt)
  }

}
