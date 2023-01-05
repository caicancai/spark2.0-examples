# Spark-example

考虑到Spark原生支持Linux，想试一下Windows跑的效果，该项目的例子全部通过本地测试

在跑之前需要 配置master URL ，可以选择直接在SparkSeesion配置

```scala
val spark = SparkSession
      .builder()
      .appName("word test")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
```

也可以在 Edit [Configuration](https://so.csdn.net/so/search?q=Configuration&spm=1001.2101.3001.7020)选项 中配置

 ![在这里插入图片描述](https://img-blog.csdnimg.cn/20190815175015432.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L29uZTExMWE=,size_16,color_FFFFFF,t_70) 

有的example需要配置config的dir文件夹路径自己配置

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
