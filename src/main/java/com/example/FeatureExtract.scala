package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object FeatureExtract {
  def main(args: Array[String]): Unit = {
//    创建spark
    val spark = SparkSession
      .builder()
      .appName("FeatureExtract App")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    val orders = spark.sql("select * from badou.orders")
    val priors = spark.sql("select * from badou.priors")
//    如果是NULL  被填充为0
//    orders.na.fill(0).show()
//    异常值处理： days_since_prior_order 空值进行处理
//    orders.selectExpr("*").show()
//    多行变为一行快捷键 ctrl+shift+j
    val ordersNew = orders.selectExpr("*"
        , "if(days_since_prior_order='',0.0,days_since_prior_order) as dspo")
        .drop("days_since_prior_order")
    ordersNew.show()

//    1、每个用户平均购买订单的间隔周期
    val userGap = ordersNew.selectExpr("user_id","cast(dspo as int) as dspo")
      .groupBy("user_id").avg("dspo").withColumnRenamed("avg(dspo)","u_avg_day_gap")

//    2、每个用户的总订单数量（分组）
    val userOrdCnt = orders.groupBy("user_id").count()
    userOrdCnt.show()

//    3、每个用户购买的product商品去重后的集合数据
//    数据结果： 1001 101200,120219,129101  user_id product_id
    val opDF = orders.join(priors, "order_id")
    val up = opDF.select("user_id","product_id")
    up.show()
//  将DF转换为RDD

    import spark.implicits._
    val rddRecords = up.rdd.map{x=>(x(0).toString, x(1).toString)}.groupByKey()
      .mapValues(_.toSet.mkString(","))
//      .mapValues{record=>record.toSet.mkString(",")}

    rddRecords.take(5)  // take作用: 获取RDD中的元素，从0到num-1 下标的元素，不排序的

//    将RDD转换为DF
    rddRecords.toDF("user_id", "product_records").show()
//    false 如果输出结果太多，不能完全展示，可以使用 false
    rddRecords.toDF("user_id", "product_records").show(3, false)

//    4、每个用户总商品数量以及去重后的商品数量(distinct count)
    val userAllProd = up.groupBy("user_id").count()

    val userUniOrdCnt = up.rdd.map{x=>(x(0).toString, x(1).toString)}
      .groupByKey()
      .mapValues(_.toSet.size)
      .toDF("user_id","pro_dist_cnt")

    userUniOrdCnt.show()

//    引入cache机制
    val userRddGroup = up.rdd.map(x=>(x(0).toString, x(1).toString)).groupByKey().cache()
    val userUnioOrdRecs = userRddGroup.mapValues(_.toSet.mkString(",")).toDF("user_id","product_records")
    val userUnioOrdCnt= userRddGroup.mapValues(_.toSet.size).toDF("user_id","pro_dist_cnt")
//   python del userRddGroup 内存中清除
//    userRddGroup.unpersist()


// 方式二：同时进行计算
//   在scala中函数的最后一行为整个函数的返回值
    val userProRcdSize = up.rdd.map{x=>(x(0).toString, x(1).toString)}
      .groupByKey()
      .mapValues{records=>
        val rs = records.toSet;
        (rs.size, rs.mkString(","))
      }.toDF("user_id","tuple")
      .selectExpr("user_id","tuple._1 as pro_dist_size","tuple._2 as prod_records")
      .filter(col("user_id") === "1").show(1,false)

//    userProRcdSize.show()
//方式三：使用agg函数

    val usergroup = up.groupBy("user_id")
      .agg(size(collect_set("product_id")).as("prod_dist_size")
      ,collect_set("product_id").as("prod_records"))
      .filter(col("user_id") === "1").show(1,false)
//    usergroup.show()

//    5、每个用户一个订单平均是多少商品
//   a、先计算每个订单有多少商品 分子
    val ordProdCnt = priors.groupBy("order_id").count()
//    ordProdCnt.show()

//    b、再求每个用户订单商品数量的平均值
    val userPerOrdCnt = orders.join(ordProdCnt, "order_id")
      .groupBy("user_id")
      .agg(avg("count")).withColumnRenamed("avg(count)","u_avg_ord_prods")

//    userPerOrdCnt.show()

  }

}
