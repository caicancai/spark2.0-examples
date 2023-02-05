package com.example



import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Usage: BroadcastTest [slices] [numElem] [blockSize]
  */
object BroadcastTest {
  def main(args: Array[String]) {

    val blockSize = if (args.length > 2) args(2) else "4096"

    val spark = SparkSession
      .builder()
      .appName("Broadcast Test")
      .config("spark.broadcast.blockSize", blockSize)
      .getOrCreate()

    val sc = spark.sparkContext

    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = (0 until num).toArray

    for (i <- 0 until 3) {
      println("Iteration " + i)
      println("===========")

      //返回最准确的可用系统计时器的当前值，以毫微秒为单位。
      //此方法只能用于测量已过的时间，与系统或钟表时间的其他任何时间概念无关
      val startTime = System.nanoTime

      //广播变量允许程序员在每台计算机上缓存只读变量，而不是将其副本与任务一起发送
      val barr1 = sc.broadcast(arr1)

      val observedSizes = sc.parallelize(1 to 10, slices).map(_ => barr1.value.length)

      // 收集小的RDD，这样我们就可以在本地打印观察到的大小
      observedSizes.collect().foreach(i => println(i))
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))
    }

    spark.stop()
  }
}
// scalastyle:on println
