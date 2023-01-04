# Spark-example

考虑到Spark原生支持Linux，想试一下Windows跑的效果，该项目的例子全部通过本地测试

如果想要在linux上跑只需要改一下SparkSession配置，注意config的dir文件夹路径自己配置

```scala
val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .config("spark.sql.warehouse.dir", null)
      .getOrCreate()
```

如果文件是windows本地，需要在文件路径前加file:\\\\\

```string
file:///E:\spark\spark_learning\src\main\resources\\people.csv
```

