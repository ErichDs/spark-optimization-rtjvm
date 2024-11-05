package part3dfjoins

import org.apache.spark.sql.SparkSession

object Bucketing {

  val spark = SparkSession.builder()
    .appName("Bucketing")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  // deactivate broadcasting
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  val large = spark.range(1000000).selectExpr("id * 5 as id").repartition(10)
  val small = spark.range(10000).selectExpr("id * 3 as id").repartition(3)

  val joined = large.join(small, "id")
  joined.explain()
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Project [id#2L]
     +- SortMergeJoin [id#2L], [id#6L], Inner
        :- Sort [id#2L ASC NULLS FIRST], false, 0
        :  +- Exchange hashpartitioning(id#2L, 200), ENSURE_REQUIREMENTS, [plan_id=33]
        :     +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=25]
        :        +- Project [(id#0L * 5) AS id#2L]
        :           +- Range (0, 1000000, step=1, splits=1)
        +- Sort [id#6L ASC NULLS FIRST], false, 0
           +- Exchange hashpartitioning(id#6L, 200), ENSURE_REQUIREMENTS, [plan_id=34]
              +- Exchange RoundRobinPartitioning(3), REPARTITION_BY_NUM, [plan_id=28]
                 +- Project [(id#4L * 3) AS id#6L]
                    +- Range (0, 10000, step=1, splits=1)
   */

  // bucketing
  large.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_large")

  small.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_small") // bucketing and saving almost as expensive as a regular shuffle

  val bucketedLarge = spark.table("bucketed_large")
  val bucketedSmall = spark.table("bucketed_small")
  val bucketedJoin = bucketedLarge.join(bucketedSmall, "id")
  bucketedJoin.explain()
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Project [id#11L]
     +- SortMergeJoin [id#11L], [id#13L], Inner
        :- Sort [id#11L ASC NULLS FIRST], false, 0
        :  +- Filter isnotnull(id#11L)
        :     +- FileScan parquet default.bucketed_large[id#11L] Batched: true, Bucketed: true, DataFilters: [isnotnull(id#11L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
        +- Sort [id#13L ASC NULLS FIRST], false, 0
           +- Filter isnotnull(id#13L)
              +- FileScan parquet default.bucketed_small[id#13L] Batched: true, Bucketed: true, DataFilters: [isnotnull(id#13L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
   */

  // bucketing for groups
  val flightsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/flights/flights.json")
    .repartition(2)

  val mostDelayed = flightsDF
    .filter("origin = 'DEN' and arrdelay > 1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last)

  mostDelayed.explain()
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Sort [avg(arrdelay)#64 DESC NULLS LAST], true, 0
     +- Exchange rangepartitioning(avg(arrdelay)#64 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=159]
        +- HashAggregate(keys=[origin#37, dest#34, carrier#28], functions=[avg(arrdelay#27)])
           +- Exchange hashpartitioning(origin#37, dest#34, carrier#28, 200), ENSURE_REQUIREMENTS, [plan_id=156]
              +- HashAggregate(keys=[origin#37, dest#34, carrier#28], functions=[partial_avg(arrdelay#27)])
                 +- Exchange RoundRobinPartitioning(2), REPARTITION_BY_NUM, [plan_id=152]
                    +- Filter (((isnotnull(origin#37) AND isnotnull(arrdelay#27)) AND (origin#37 = DEN)) AND (arrdelay#27 > 1.0))
                       +- FileScan json [arrdelay#27,carrier#28,dest#34,origin#37] Batched: false, DataFilters: [isnotnull(origin#37), isnotnull(arrdelay#27), (origin#37 = DEN), (arrdelay#27 > 1.0)], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(origin), IsNotNull(arrdelay), EqualTo(origin,DEN), GreaterThan(arrdelay,1.0)], ReadSchema: struct<arrdelay:double,carrier:string,dest:string,origin:string>
   */

//  flightsDF.write
//    .partitionBy("origin")
//    .bucketBy(4, "dest", "carrier")
//    .saveAsTable("flights_bucketed") // just as long as shuffle
//
//  val flightsBucketed = spark.table("flights_bucketed")
//  val mostDelayed2 = flightsBucketed
//    .filter("origin = 'DEN' and arrdelay > 1")
//    .groupBy("origin", "dest", "carrier")
//    .avg("arrdelay")
//    .orderBy($"avg(arrdelay)".desc_nulls_last)
//
//  mostDelayed2.explain()
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Sort [avg(arrdelay)#160 DESC NULLS LAST], true, 0
     +- Exchange rangepartitioning(avg(arrdelay)#160 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=205]
        +- HashAggregate(keys=[origin#133, dest#130, carrier#124], functions=[avg(arrdelay#123)])
           +- HashAggregate(keys=[origin#133, dest#130, carrier#124], functions=[partial_avg(arrdelay#123)])
              +- Filter (isnotnull(arrdelay#123) AND (arrdelay#123 > 1.0))
                 +- FileScan parquet default.flights_bucketed[arrdelay#123,carrier#124,dest#130,origin#133] Batched: true, Bucketed: true, DataFilters: [isnotnull(arrdelay#123), (arrdelay#123 > 1.0)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [isnotnull(origin#133), (origin#133 = DEN)], PushedFilters: [IsNotNull(arrdelay), GreaterThan(arrdelay,1.0)], ReadSchema: struct<arrdelay:double,carrier:string,dest:string>, SelectedBucketsCount: 4 out of 4
   */

  /**
    * Bucket pruning
    */
    val the10 = bucketedLarge.filter($"id" === 10)
    the10.show()
    the10.explain()
  /*
  == Physical Plan ==
  *(1) Filter (isnotnull(id#11L) AND (id#11L = 10))
  +- *(1) ColumnarToRow
     +- FileScan parquet default.bucketed_large[id#11L] Batched: true, Bucketed: false (disabled by query planner), DataFilters: [isnotnull(id#11L), (id#11L = 10)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(id), EqualTo(id,10)], ReadSchema: struct<id:bigint>
   */

  def main(args: Array[String]): Unit = {
//    joined.count() // 4-5s
//    bucketedJoin.count() // almost 4s-5s for bucketing + 0.4s for counting
//    mostDelayed.show() // ~1s
//    mostDelayed2.show() // ~0.2s
  }
}
