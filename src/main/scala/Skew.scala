import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession


object Skew {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("dude, i need at least one parameter")
    }

    val iter = args(0).toInt
    val numKeys = 1024 * 1024 * 16 * iter
    val numInputBins = 203 * iter
    val numPartitions = 23 * iter

    val (weightToPartition, totalWeight) = {
      // assumes numInputBins > 21
      val expensivePartitions = Array(0, 1, 11, 21)

      // simulate skew
      val bins = Array.tabulate(numInputBins)(i => 1)
      for (p <- expensivePartitions) bins(p) = numInputBins * 4 - p

      var count = 0
      val map = new java.util.TreeMap[Int, Int]()
      for (p <- 0 until numInputBins) {
        map.put(count, p)
        count += bins(p)
      }

      (map, count)
    }

    // Some random computations which should result in very bad skew
    val sc = SparkSession.builder().getOrCreate().sparkContext

    val rdd = sc.parallelize(0 until numKeys, numPartitions).mapPartitions {
      iter => {

        val random = new scala.util.Random(TaskContext.get().partitionId())

        def generate(): Int = {
          weightToPartition.floorKey(random.nextInt(totalWeight))
        }

        iter.map {
          v => (generate(), v)
        }
      }
    }

    val grouped = rdd.groupByKey()

    val regrouped = grouped.flatMap {
      v => {
        val key = v._1
        v._2.map {
          v1 => (key, v1)
        }
      }
    }.combineByKey(createCombiner = (v: Int) => v.toLong,
      mergeValue = (c: Long, v: Int) => c + v,
      mergeCombiners = (c1: Long, c2: Long) => c1 + c2)

    implicit val ord: Ordering[(Int, Long)] = Ordering.by((keyValue: (Int, Long)) => keyValue._2)
    val highestKeyValue = regrouped.max()

  }
}
