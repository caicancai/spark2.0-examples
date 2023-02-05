package com.rdd

import org.apache.spark.sql.SparkSession

object CreateEmptyRDD {
  def main(args: Array[String]): Unit ={
    val spark:SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val rdd = spark.sparkContext.emptyRDD
    val rddString = spark.sparkContext.emptyRDD[String]

    println(rdd)
    println(rddString)
    println("Num of Partitions: "+rdd.getNumPartitions)

    val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
    println(rdd2)
    println("Num of Partitions: "+rdd2.getNumPartitions)

    rdd2.saveAsTextFile("c:/tmp/test3.txt")

    // Pair RDD

    type dataType = (String,Int)
    var pairRDD = spark.sparkContext.emptyRDD[dataType]
    println(pairRDD)
  }


}
