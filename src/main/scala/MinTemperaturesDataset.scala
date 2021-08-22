import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, FloatType, StructType}
import org.apache.spark.sql.functions._

/** Find the minimum temperature by weather station */
object MinTemperaturesDataset {

  case class Temperature(stationID: String, date: Int, measure_type: String, temperature: Float)

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Open up the Spark Session of the application
    val spark = SparkSession
      .builder
      .appName("MinTemperatures")
      .master("local[*]")
      .getOrCreate()

    // Define Temperature schema (when you do not have header in a CSV file)
    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)

    // Read each line of input data
    import spark.implicits._
    val lines = spark.read
      .schema(temperatureSchema) // not inferring the schema
      .csv("./data/1800.csv")
      .as[Temperature]

    // Filter measures related to minimum temperatures
    val minTemps = lines.filter($"measure_type" === "TMIN")
    val stationTemps = minTemps.select("stationID", "temperature")

    // Get the minimum temperature by weather station
    val minTempsByStation = stationTemps.groupBy("stationID").min("temperature")

    // Convert to Fahrenheit and prettify results
    val minTempsByStationF = minTempsByStation
      .withColumn("temperature", round($"min(temperature)" * 0.1f * (9.0f / 5.0f) + 32.0f, 2))
      .select("stationID", "temperature")

    // Show all results
    minTempsByStationF.show(minTempsByStationF.count.toInt)

  }
}