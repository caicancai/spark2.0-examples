package com.example

import org.apache.spark.sql.SparkSession

object LocalTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("word test")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    val file = spark.sparkContext.textFile("E:\\大数据环境搭建\\1.txt")
    file.flatMap(line=>line.split(" ")).map((_,1)).reduceByKey(_+_)
      .take(10).foreach(println(_))

  }
}
