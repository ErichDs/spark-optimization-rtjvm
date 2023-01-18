package part2foundations

import org.apache.spark.sql.{SaveMode, SparkSession}

object TestDeployApp {

  // TestDeployApp inputFile outputFile
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Need input file and output file")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Test Deploy App")
      // method 1
      .config("spark.executor.memory", "1g")
      .getOrCreate()

    import spark.implicits._

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(args(0))

    val goodComediesDF = moviesDF.select(
      $"Title"
      ,$"IMDB_Rating".as("Rating")
      ,$"Release_Date".as("Release")
    )
      .where(($"Major_Genre" === "Comedy") && ($"IMDB_Rating" > 6.5))
      .orderBy($"Rating".desc_nulls_last)

    // method 2
    spark.conf.set("spark.executor.memory", "1g") // warning - not all configurations available this way

    goodComediesDF.show()

    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))
  }

  /**
    * After building your application;
    * - open a terminal and start your cluster: docker-compose up --scale spark-worker=3
    * - open another terminal or tab and connect to the cluster:
    *   docker exec -it your-spark-cluster-master-1 bash
    *
    * - go to the /spark/bin and submit you application:
    *
    *  // method number 3 and 3.1 in line 54:
    *
    *     spark-submit \
    *     --class part2foundations.TestDeployApp  \
    *     --master spark://(dockerID)):7077  \
    *     -- conf spark.executor.memory 1g \\ or -- executor-memory 1g
          --deploy-mode client  \
          --verbose \
          --supervise \
          /opt/spark-apps/spark-optimization.jar /opt/spark-data/movies.json /opt/spark-data/goodComedies.json
    *
    */

}
