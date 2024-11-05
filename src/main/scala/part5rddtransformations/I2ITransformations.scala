package part5rddtransformations

import generator.DataGenerator
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
object I2ITransformations {

  val spark = SparkSession
    .builder()
    .appName("I2I Transformations")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
   * Science Project
   * each metric has identifier, value
   *
   * Return the smallest ("best") 10 metrics (ifentifiers + values)
   * */

  val LIMIT = 10

  def readMetrics() =
    sc.textFile("src/main/resources/generated/metrics/metrics10m.txt").map { line =>
      val tokens = line.split(" ")
      val name   = tokens(0)
      val value  = tokens(1).toDouble

      (name, value)
    }

  def printTopMetrics() = {
    val sortedMetrics =
      readMetrics().sortBy(_._2).take(LIMIT) // deterministic because the RDD is sorted
    sortedMetrics.foreach(println)
  }

  // iterator transformations
  def printTopMetricsI2I() = {

    val iteratorToIteratorTransformation = (records: Iterator[(String, Double)]) => {
      /*
          i2i transformation
          - they are NARROW TRANSFORMATIONS
          - Spark will "selectively" spill data to disk when partitions are too big for memory

          Warning: don't traverse more than once or convert to collections (otherwise spark will load everything to memory)
       */

      implicit val ordering: Ordering[(String, Double)] =
        Ordering.by[(String, Double), Double](_._2)
      val limitedCollection = new mutable.TreeSet[(String, Double)]()

      records.foreach { record =>
        limitedCollection.add(record)
        if (limitedCollection.size > LIMIT) {
          limitedCollection.remove(limitedCollection.last)
        }
      }

      // I've traversed the iterator

      limitedCollection.toIterator
    }

    val topMetrics = readMetrics()
      .mapPartitions(iteratorToIteratorTransformation)
      .repartition(1)
      .mapPartitions(iteratorToIteratorTransformation)

    val result = topMetrics.take(LIMIT)
    result.foreach(println)
  }

  def main(args: Array[String]): Unit = {
//    DataGenerator.generateMetrics("src/main/resources/generated/metrics/metrics10m.txt", 10000000)

    printTopMetrics()
    printTopMetricsI2I()
    Thread.sleep(1000000)
  }
}
