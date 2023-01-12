package part1recap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkRecap {

  // the entry point to the Spark structured API
  val spark = SparkSession.builder()
    .appName("Spark Recap")
    .master("local[2]")
    .getOrCreate()

  // read a DF
  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars/cars.json")

  import spark.implicits._

  // select
  val usefulCarsDF = carsDF.select(
    col("Name"), // column object
    $"Year", // another column object (needs spark implicits)
    (col("Weight_in_lbs") / 2.2).as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  val carsWeightsDF = carsDF.selectExpr("Weight_in_lbs / 2.2")

  // filter
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")

  // aggregations
  val averageHP = carsDF.select(avg(col("Horsepower")).as("average_hp")) // sum, mean, stddev, min, max

  // grouping
  val countByOrigin = carsDF
    .groupBy(col("Origin")) // a RelationalGroupedDataset
    .count()

  // joining
  val guitarPlayers = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers")

  val bands = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands")

  val guitaristsBands = guitarPlayers.
    join(bands, guitarPlayers.col("band") === bands.col("id"), "inner")
  /*
  join types
   - inner: only the matching rows are kept
   - left/right/full outer join
   - semi/anti
   */

  // datasets
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  // datasets = typed distributed collections of objects
  val guitarPlayersDS = guitarPlayers.as[GuitarPlayer] // needs spark.implicits, contains encoders so that
  guitarPlayersDS.map(_.name)                          // the rows of a DF are safely converted to the JVM objects that you want to be typed with.

  // spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCars = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )

  // low-level API: RDDs
  val sc = spark.sparkContext
  val numbersRDD: RDD[Int] = sc.parallelize(1 to 1000000)

  // functional operators
  numbersRDD.map(_ * 2)

  // RDD -> DF - you lose type info, but you get SQL capability
  val numbersDF = numbersRDD.toDF("number")

  // RDD -> DS
  val numberDS = spark.createDataset(numbersRDD)

  // DS -> RDD
  val guitarPlayersRDD = guitarPlayersDS.rdd

  // DF -> RDD
  val carsRDD = carsDF.rdd // RDD[Row]

  def main(args: Array[String]): Unit = {
    // showing a DF to the console
    carsDF.show()
    carsDF.printSchema()

  }
}
