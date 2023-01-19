package part3dfjoins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnPruning {

  val spark = SparkSession.builder()
    .appName("Column Pruning")
    .master("local[2]")
    .getOrCreate()

  val sc = spark.sparkContext
  import spark.implicits._

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars/guitars.json")

  val guitarPlayersDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands/bands.json")

  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitarPlayersDF.join(bandsDF, joinCondition) // inner implied
  guitaristsBandsDF.explain()

  /* // I don't have the 'project' line below
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- BroadcastHashJoin [band#24L], [id#41L], Inner, BuildLeft, false
     :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]),false), [plan_id=30]
    :  +- Filter isnotnull(band#24L)
    :     +- FileScan json [band#24L,guitars#25,id#26L,name#27] Batched: false, DataFilters: [isnotnull(band#24L)], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,guitars:array<bigint>,id:bigint,name:string>
    +- Filter isnotnull(id#41L)
        +- FileScan json [hometown#40,id#41L,name#42,year#43L] Batched: false, DataFilters: [isnotnull(id#41L)], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<hometown:string,id:bigint,name:string,year:bigint>
                               /\ UNNECESSARY FIELDS
   */

  val guitaristsWithoutBandsDF = guitarPlayersDF.join(bandsDF, joinCondition, "left_anti")
  guitaristsWithoutBandsDF.explain()
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- BroadcastHashJoin [band#24L], [id#41L], LeftAnti, BuildRight, false
     :- FileScan json [band#24L,guitars#25,id#26L,name#27] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<band:bigint,guitars:array<bigint>,id:bigint,name:string>
    +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]),false), [plan_id=49]
        +- Filter isnotnull(id#41L)
           +- FileScan json [id#41L] Batched: false, DataFilters: [isnotnull(id#41L)], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>
                               /\ COLUMN PRUNING

  Column pruning = cut off columns that are not relevant
  = shrinks DF
  * useful for joins and groups
   */

  // project and filter pushdown
  val namesDF = guitaristsBandsDF.select(guitarPlayersDF.col("name"), bandsDF.col("name"))
  namesDF.explain()
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Project [name#27, name#42]
     +- BroadcastHashJoin [band#24L], [id#41L], Inner, BuildRight, false
        :- Filter isnotnull(band#24L)
        :  +- FileScan json [band#24L,name#27] Batched: false, DataFilters: [isnotnull(band#24L)], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,name:string>
        +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]),false), [plan_id=77]
           +- Filter isnotnull(id#41L)
              +- FileScan json [id#41L,name#42] Batched: false, DataFilters: [isnotnull(id#41L)], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,name:string>

  Spark tends to drop columns as early as possible.
  Should be YOUR goal as well.
  */

  val rockDF = guitarPlayersDF
    .join(bandsDF, joinCondition)
    .join(guitarsDF, array_contains(guitarPlayersDF.col("guitars"), guitarsDF.col("id")))

  val essentialsDF = rockDF.select(guitarPlayersDF.col("name"), bandsDF.col("name"), upper(guitarsDF.col("make")))
  essentialsDF.explain()
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Project [name#27, name#42, upper(make#10) AS upper(make)#117]
     +- BroadcastNestedLoopJoin BuildRight, Inner, array_contains(guitars#25, id#9L)
        :- Project [guitars#25, name#27, name#42]
        :  +- BroadcastHashJoin [band#24L], [id#41L], Inner, BuildRight, false
        :     :- Filter (isnotnull(band#24L) AND isnotnull(guitars#25))
        :     :  +- FileScan json [band#24L,guitars#25,name#27] Batched: false, DataFilters: [isnotnull(band#24L), isnotnull(guitars#25)], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(band), IsNotNull(guitars)], ReadSchema: struct<band:bigint,guitars:array<bigint>,name:string>
        :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]),false), [plan_id=124]
        :        +- Filter isnotnull(id#41L)
        :           +- FileScan json [id#41L,name#42] Batched: false, DataFilters: [isnotnull(id#41L)], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,name:string>
        +- BroadcastExchange IdentityBroadcastMode, [plan_id=128]
           +- Filter isnotnull(id#9L)
              +- FileScan json [id#9L,make#10] Batched: false, DataFilters: [isnotnull(id#9L)], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimiz..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,make:string>
   */

  /**
    * LESSON: if you anticipate that the joined table is much larger than the table on whose column you are applying
    * the map-side operation, e.g. " * 5", or "upper", do this operation on the small table FIRST.
    *
    * Particular useful for outer joins.
    */

  def main(args: Array[String]): Unit = {

  }
}
