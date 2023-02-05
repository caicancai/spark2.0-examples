package com.example.sql

import org.apache.spark.sql.SaveMode
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$

// One method for defining the schema of an RDD is to make a case class with the desired column
// names and types.
case class Record(key: Int, value: String)

object RDDRelation {
  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[2]")
      .config("spark.sql.warehouse.dir","E:\\spark\\badou_spark_26\\badou_spark_26\\spark-warehouse")
      .getOrCreate()

    // Importing the SparkSession gives access to all the SQL functions and implicit conversions.
    import spark.implicits._
    // $example off:init_session$

    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    // 任何包含case类的RDD都可以用来创建临时视图。视图的模式是使用scala反射自动推断的。
    df.createOrReplaceTempView("records")

    // 一旦注册了表，就可以对它们运行SQL查询。
    println("Result of SELECT *:")
    spark.sql("SELECT * FROM records").collect().foreach(println)


    //还支持聚合查询。
    val count = spark.sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // SQL查询的结果本身就是RDD，并且支持所有正常的RDD函数。RDD中的项目类型为Row，允许您按顺序访问每一列。
    val rddFromSql = spark.sql("SELECT key, value FROM records WHERE key < 10")

    println("Result of RDD.map:")
    rddFromSql.rdd.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)

    // Queries can also be written using a LINQ-like Scala DSL.
    println("===========================================================")
    df.select($"key").where($"key" === 1).orderBy($"value".asc).collect().foreach(println)
    println("===========================================================")
    // Write out an RDD as a parquet file with overwrite mode.
    df.write.mode(SaveMode.Overwrite).parquet("pair.parquet")

    // Read in parquet file.  Parquet files are self-describing so the schema is preserved.
    val parquetFile = spark.read.parquet("pair.parquet")

    // Queries can be run using the DSL on parquet files just like the original RDD.
    parquetFile.where($"key" === 1).select($"value".as("a")).collect().foreach(println)

    // These files can also be used to create a temporary view.
    parquetFile.createOrReplaceTempView("parquetFile")
    spark.sql("SELECT * FROM parquetFile").collect().foreach(println)

    spark.stop()
  }
}
// scalastyle:on println
