package part3dfjoins

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object PrePartitioning {

  val spark = SparkSession.builder()
    .appName("Pre-Partitioning")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  // deactivate broadcast joins
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  /*
  addColumns(initialTable, 3) => df with columns "id", "newCol1", "newCol2", "newCol3"
   */
  def addColumns[T](df: Dataset[T], n: Int): DataFrame = {
    val newColumns = (1 to n).map(i => s"id * $i as newCol$i")
    df.selectExpr(("id" +: newColumns): _*)
  }

  // don't touch this
  val initialTable = spark.range(1, 10000000).repartition(10) // RoundRobinPartitioning(10)
  val narrowTable = spark.range(1, 5000000).repartition(7) // RoundRobinPartitioning(7)

  // scenario 1
  val wideTable = addColumns(initialTable, 30)
  val join1 = wideTable.join(narrowTable, "id")
//  println(join1.count()) // around 17s
  join1.explain()
  /*
    == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Project [id#0L, newCol1#8L, newCol2#9L, newCol3#10L, newCol4#11L, newCol5#12L, newCol6#13L, newCol7#14L, newCol8#15L, newCol9#16L, newCol10#17L, newCol11#18L, newCol12#19L, newCol13#20L, newCol14#21L, newCol15#22L, newCol16#23L, newCol17#24L, newCol18#25L, newCol19#26L, newCol20#27L, newCol21#28L, newCol22#29L, newCol23#30L, ... 7 more fields]
     +- SortMergeJoin [id#0L], [id#4L], Inner
        :- Sort [id#0L ASC NULLS FIRST], false, 0
        :  +- Exchange hashpartitioning(id#0L, 200), ENSURE_REQUIREMENTS, [plan_id=29]
        :     +- Project [id#0L, (id#0L * 1) AS newCol1#8L, (id#0L * 2) AS newCol2#9L, (id#0L * 3) AS newCol3#10L, (id#0L * 4) AS newCol4#11L, (id#0L * 5) AS newCol5#12L, (id#0L * 6) AS newCol6#13L, (id#0L * 7) AS newCol7#14L, (id#0L * 8) AS newCol8#15L, (id#0L * 9) AS newCol9#16L, (id#0L * 10) AS newCol10#17L, (id#0L * 11) AS newCol11#18L, (id#0L * 12) AS newCol12#19L, (id#0L * 13) AS newCol13#20L, (id#0L * 14) AS newCol14#21L, (id#0L * 15) AS newCol15#22L, (id#0L * 16) AS newCol16#23L, (id#0L * 17) AS newCol17#24L, (id#0L * 18) AS newCol18#25L, (id#0L * 19) AS newCol19#26L, (id#0L * 20) AS newCol20#27L, (id#0L * 21) AS newCol21#28L, (id#0L * 22) AS newCol22#29L, (id#0L * 23) AS newCol23#30L, ... 7 more fields]
        :        +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=21]
        :           +- Range (1, 10000000, step=1, splits=1)
        +- Sort [id#4L ASC NULLS FIRST], false, 0
           +- Exchange hashpartitioning(id#4L, 200), ENSURE_REQUIREMENTS, [plan_id=30]
              +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=24]
                 +- Range (1, 5000000, step=1, splits=1)
   */

  // scenario 2
  val altNarrow = narrowTable.repartition($"id") // use a HashPartitioning
  val altInitial = initialTable.repartition($"id") // same partitioner
  //join on co-partitioned DFs
  val join2 = altInitial.join(altNarrow, "id")
  val result2 = addColumns(join2, 30)
//  println(result2.count()) // around 4s
  result2.explain()
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Project [id#0L, (id#0L * 1) AS newCol1#105L, (id#0L * 2) AS newCol2#106L, (id#0L * 3) AS newCol3#107L, (id#0L * 4) AS newCol4#108L, (id#0L * 5) AS newCol5#109L, (id#0L * 6) AS newCol6#110L, (id#0L * 7) AS newCol7#111L, (id#0L * 8) AS newCol8#112L, (id#0L * 9) AS newCol9#113L, (id#0L * 10) AS newCol10#114L, (id#0L * 11) AS newCol11#115L, (id#0L * 12) AS newCol12#116L, (id#0L * 13) AS newCol13#117L, (id#0L * 14) AS newCol14#118L, (id#0L * 15) AS newCol15#119L, (id#0L * 16) AS newCol16#120L, (id#0L * 17) AS newCol17#121L, (id#0L * 18) AS newCol18#122L, (id#0L * 19) AS newCol19#123L, (id#0L * 20) AS newCol20#124L, (id#0L * 21) AS newCol21#125L, (id#0L * 22) AS newCol22#126L, (id#0L * 23) AS newCol23#127L, ... 7 more fields]
     +- SortMergeJoin [id#0L], [id#4L], Inner
        :- Sort [id#0L ASC NULLS FIRST], false, 0
        :  +- Exchange hashpartitioning(id#0L, 200), REPARTITION_BY_COL, [plan_id=53]
        :     +- Range (1, 10000000, step=1, splits=1)
        +- Sort [id#4L ASC NULLS FIRST], false, 0
           +- Exchange hashpartitioning(id#4L, 200), REPARTITION_BY_COL, [plan_id=55]
              +- Range (1, 5000000, step=1, splits=1)
   */

  // scenario 3
  val enhancedColumnsFirst = addColumns(initialTable, 30)
  val repartitionedNarrow = narrowTable.repartition($"id")
  val repartitionedEnhanced = enhancedColumnsFirst.repartition($"id") // USELESS! If you comment this line, the query plan will remain the same.
  val result3 = repartitionedEnhanced.join(repartitionedNarrow, "id")
//  println(result3.count()) // around 17s
  result3.explain()

  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Project [id#0L, newCol1#238L, newCol2#239L, newCol3#240L, newCol4#241L, newCol5#242L, newCol6#243L, newCol7#244L, newCol8#245L, newCol9#246L, newCol10#247L, newCol11#248L, newCol12#249L, newCol13#250L, newCol14#251L, newCol15#252L, newCol16#253L, newCol17#254L, newCol18#255L, newCol19#256L, newCol20#257L, newCol21#258L, newCol22#259L, newCol23#260L, ... 7 more fields]
     +- SortMergeJoin [id#0L], [id#4L], Inner
        :- Sort [id#0L ASC NULLS FIRST], false, 0
        :  +- Exchange hashpartitioning(id#0L, 200), REPARTITION_BY_COL, [plan_id=531]
        :     +- Project [id#0L, (id#0L * 1) AS newCol1#238L, (id#0L * 2) AS newCol2#239L, (id#0L * 3) AS newCol3#240L, (id#0L * 4) AS newCol4#241L, (id#0L * 5) AS newCol5#242L, (id#0L * 6) AS newCol6#243L, (id#0L * 7) AS newCol7#244L, (id#0L * 8) AS newCol8#245L, (id#0L * 9) AS newCol9#246L, (id#0L * 10) AS newCol10#247L, (id#0L * 11) AS newCol11#248L, (id#0L * 12) AS newCol12#249L, (id#0L * 13) AS newCol13#250L, (id#0L * 14) AS newCol14#251L, (id#0L * 15) AS newCol15#252L, (id#0L * 16) AS newCol16#253L, (id#0L * 17) AS newCol17#254L, (id#0L * 18) AS newCol18#255L, (id#0L * 19) AS newCol19#256L, (id#0L * 20) AS newCol20#257L, (id#0L * 21) AS newCol21#258L, (id#0L * 22) AS newCol22#259L, (id#0L * 23) AS newCol23#260L, ... 7 more fields]
        :        +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=529]
        :           +- Range (1, 10000000, step=1, splits=1)
        +- Sort [id#4L ASC NULLS FIRST], false, 0
           +- Exchange hashpartitioning(id#4L, 200), REPARTITION_BY_COL, [plan_id=533]
              +- Range (1, 5000000, step=1, splits=1)
   */

  /**
    * Lesson: partition early.
    * Partitioning late is AT BEST what Spark naturally does.
    */

  /**
    * Exercise: what would happen if we just repartitioned the smaller table to 10 partitions?
    * TERRIBLE!
    *
    */

    initialTable.join(narrowTable.repartition(10), "id").explain() // identical to scenario 1

  def main(args: Array[String]): Unit = {

  }
}
