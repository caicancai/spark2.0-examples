# RDD编程指南

## 概述

每个Spark应用程序都包含一个驱动程序，该程序运行用户的功能并在群集上执行各种*并行操作*。Spark 提供的主要抽象是一个*弹性分布式数据集* （RDD），它是跨群集节点划分的元素集合，可以并行操作。RDD是通过从Hadoop文件系统（或任何其他Hadoop支持的文件系统）中的文件或驱动程序中的现有Scala集合开始，然后对其进行转换来创建的。用户还可以要求Spark将RDD*保留*在内存中，从而允许在并行操作中有效地重用RDD。最后，RDD 会自动从节点故障中恢复。 

 Spark 中的第二个抽象是可以在并行操作中使用的*共享变量*。默认情况下，当 Spark 在不同节点上将函数作为一组任务并行运行时，它会将函数中使用的每个变量的副本传送到每个任务。有时，需要在任务之间或在任务和驱动程序之间共享变量。Spark 支持两种类型的共享变量：广播变量（可用于在所有节点上的内存中缓存值）和*累加器*（仅“添加”到的*变量*，例如计数器和总和） 

## 初始化Spark

 Spark程序必须做的第一件事是创建一个[SparkContext](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/SparkContext.html)对象，它告诉Spark 如何访问集群。要创建一个，你首先需要构建一个[SparkConf](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/SparkConf.html)对象 包含有关应用程序的信息。 

 每个 JVM 只能有一个 SparkContext 处于活动状态。在创建新 SparkContext 之前，必须执行SparkContext。

```scala
val conf = new SparkConf().setAppName(appName).setMaster(master)
new SparkContext(conf)
```

## 弹性分布式数据集（RDD）

 RDD 是可以并行操作的元素的容错集合。有两种方法可以创建 RDD：*并行化*驱动程序中的现有集合，或引用外部存储系统中的数据集，例如 共享文件系统、HDFS、HBase 或任何提供 Hadoop InputFormat 的数据源。 

### 并行集合

 并行化集合是通过在驱动程序（Scala）中的现有集合上调用 的方法创建的。复制集合的元素以形成可以并行操作的分布式数据集。例如，下面介绍如何创建包含数字 1 到 5 的并行化集合 

```scala
val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)
```

 并行集合的一个重要参数是要将数据集切入的*分区*数。Spark 将为群集的每个分区运行一个任务。通常，您希望群集中的每个 CPU 有 2-4 个分区。通常，Spark 会尝试根据群集自动设置分区数。但是，您也可以通过将其作为第二个参数传递给 它： `parallelize``sc.parallelize(data, 10)` 

### 外部数据集

 Spark可以从Hadoop支持的任何存储源创建分布式数据集，包括本地文件系统，HDFS，Cassandra，HBase，[Amazon S3](http://wiki.apache.org/hadoop/AmazonS3)等。Spark支持文本文件，[SequenceFiles](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapred/SequenceFileInputFormat.html)和任何其他Hadoop [InputFormat](http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapred/InputFormat.html)。 

 可以使用 的方法创建文本文件 RDD。此方法获取文件的 URI（计算机上的本地路径或 、 等 URI），并将其作为行的集合读取。下面是一个调用示例： 

```scala
scala> val distFile = sc.textFile("data.txt")
distFile: org.apache.spark.rdd.RDD[String] = data.txt MapPartitionsRDD[10] at textFile at <console>:26
```

-  创建后，可以通过数据集操作进行操作。例如，我们可以使用 and 运算将所有行的大小相加，如下所示： .`distFile``map``reduce``distFile.map(s => s.length).reduce((a, b) => a + b)` 

- Spark 的所有基于文件的输入法（包括 ）都支持在目录、压缩文件和通配符上运行。读取多个文件时，分区的顺序取决于文件从文件系统返回的顺序。例如，它可能会也可能不会遵循按路径排列文件的字典顺序。在分区中，元素根据其在基础文件中的顺序进行排序。`textFile``textFile("/my/directory")``textFile("/my/directory/*.txt")``textFile("/my/directory/*.gz")`
- 该方法还采用可选的第二个参数来控制文件的分区数。默认情况下，Spark 为文件的每个块创建一个分区（在 HDFS 中默认块为 128MB），但您也可以通过传递更大的值来请求更多的分区。请注意，分区数不能少于块数。

 除了文本文件，Spark的Scala API还支持其他几种数据格式： 

>  `SparkContext.wholeTextFiles`允许您读取包含多个小文本文件的目录，并将每个文件作为（文件名、内容）对返回。这与 相反，后者将在每个文件中每行返回一条记录。分区由数据局部性确定，在某些情况下，这可能会导致分区太少。对于这些情况，提供可选的第二个参数来控制最小数量的分区。 
>
> - 对于 [SequenceFiles](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapred/SequenceFileInputFormat.html)，请使用 SparkContext 的方法，其中 和 是文件中键和值的类型。这些应该是Hadoop的[可写](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/io/Writable.html)接口的子类，如[IntWritable](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/io/IntWritable.html)和[Text](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/io/Text.html)。此外，Spark 还允许您为一些常见的可写内容指定本机类型;例如，将自动读取可写内容和文本。`sequenceFile[K, V]``K``V``sequenceFile[Int, String]`
> - `RDD.saveAsObjectFile`并支持以由序列化的 Java 对象组成的简单格式保存 RDD。虽然这不如Avro等专用格式有效，但它提供了一种保存任何RDD的简单方法。

## RDD操作

### 编程模型

RDD 到 RDD 之间的转换，本质上是数据形态上的转换（Transformations）

在RDD的编程模型中，一共有两种算子，Transfromations类算子和Actions算子。开发者需要使用Transformations类算子 ，定义并描述数据形态的转换过程，然后调用Actions类算子，将计算结果收集起来或者物化到磁盘

在这样的编程模型下，Spark 在运行时的计算被划分为两个环节。

> 1、基于不同数据形态之间的转换，构建**计算流图**（DAG）
>
> 2、通过ACtions类算子，以**回溯的方式触发执行**这个计算流图

example：

```scala
val lines = sc.textFile("data.txt")
val lineLengths = lines.map(s => s.length)
val totalLength = lineLengths.reduce((a, b) => a + b)
```

 第一行定义来自外部文件的基本 RDD。此数据集未加载到内存中或 否则操作：只是指向文件的指针。 第二行定义为转换的结果。同样，由于懒惰，*不会*立即计算。 最后，我们运行 ，这是一个动作。此时，Spark将计算分解为任务。 在单独的机器上运行，并且每台机器同时运行其映射部分和局部归约， 仅将其答案返回给驱动程序。 

### RDD常用算子

### 创建RDD

在Spark中，创建RDD的典型方式有两种：

> 1、通过Spark.Context.parallelize在**内部数据**上创建 RDD
>
> ```scala
> import org.apache.spark.rdd.RDD
> val words: Array[String] = Array("Spark", "is", "cool")
> val rdd: RDD[String] = sc.parallelize(words)
> ```
>
> 2、通过SparkContext.textFile等从API从**外部数据**创建RDD
>
> ```scala
> import org.apache.spark.rdd.RDD
> val rootPath: String = _
> val file: String = s"${rootPath}/wikiOfSpark.txt"
> // 读取文件内容
> val lineRDD: RDD[String] = spark.sparkContext.textFile(file)
> ```

### RDD内的数据转换

map：给定映射函数 f，map(f) 以元素为粒度对 RDD 做数据转换

```scala
// 把普通RDD转换为Paired RDD
val cleanWordRDD: RDD[String] = _ // 请参考第一讲获取完整代码
val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1))
```

mapPartitions：以数据分区为粒度，使用映射函数f对RDD进行数据转换

```scala
// 把普通RDD转换为Paired RDD
 
import java.security.MessageDigest
 
val cleanWordRDD: RDD[String] = _ // 请参考第一讲获取完整代码
 
val kvRDD: RDD[(String, Int)] = cleanWordRDD.mapPartitions( partition => {
  // 注意！这里是以数据分区为粒度，获取MD5对象实例
  val md5 = MessageDigest.getInstance("MD5")
  val newPartition = partition.map( word => {
  // 在处理每一条数据记录的时候，可以复用同一个Partition内的MD5对象
    (md5.digest(word.getBytes()).mkString,1)
  })
  newPartition
})
```

flatMap：从元素到集合，再从集合到元素

> 以元素为单位，创建集合；
>
> 去掉集合“外包装”，提取集合元素。

```scala
// 读取文件内容
val lineRDD: RDD[String] = _ // 请参考第一讲获取完整代码
// 以行为单位提取相邻单词
val wordPairRDD: RDD[String] = lineRDD.flatMap( line => {
  // 将行转换为单词数组
  val words: Array[String] = line.split(" ")
  // 将单个单词数组，转换为相邻单词数组
  for (i <- 0 until words.length - 1) yield words(i) + "-" + words(i+1)
})
```

###  filter:过滤RDD

判定函数 f 的形参类型，必须与 RDD 的元素类型保持一致，而 f 的返回结果，只能是 True 或者 False。在任何一个 RDD 之上调用 filter(f)，其作用是保留 RDD 中满足 f（也就是 f 返回 True）的数据元素，而过滤掉不满足 f（也就是 f 返回 False）的数据元素。

```scala
// 定义特殊字符列表
val list: List[String] = List("&", "|", "#", "^", "@")
 
// 定义判定函数f
def f(s: String): Boolean = {
val words: Array[String] = s.split("-")
val b1: Boolean = list.contains(words(0))
val b2: Boolean = list.contains(words(1))
return !b1 && !b2 
}
 
// 使用filter(f)对RDD进行过滤
val cleanedPairRDD: RDD[String] = wordPairRDD.filter(f)
```

## 深入理解RDD

**RDD是对于数据模型的抽象，用于囊括所有内存中和磁盘中的分布式数据实体**

从薯片加工理解RDD

> 土豆工坊的每条流水线就像是分布式环境中的计算节点
>
> 不同的食材形态，如带泥的土豆、土豆切片、烘烤的土豆等等，对应的就是RDD
>
> 每一种食材形态都会依赖上一种形态，如烤熟的土豆片依赖上一个步骤的生土豆片，这种依赖关系对应的就是RDD中的dependcies属性
>
> 不同环节的加工方法对应RDD的compute属性
>
> 同一食材形态在不同流水线的具体实物，就是RDD的partitions 属性
>
> 食材按照什么规则被分配到哪条流水线，对应的就是 RDD 的 partitioner 属性。

### RDD核心特征和属性

通过刚才的例子，我们知道 RDD 具有 4 大属性，**分别是 partitions、partitioner、dependencies 和 compute 属性。正因为有了这 4 大属性的存在，让 RDD 具有分布式和容错性这两大最突出的特性**。

***首先，我们来看partition、partitioner属性***

在分布式运行环境中，RDD封装的数据在物理上散落在不同计算节点的内存或是磁盘中，这些散落的数据被称“数据分片”，RDD 的分区规则决定了哪些数据分片应该散落到哪些节点中去。RDD 的 partitions 属性对应着 RDD 分布式数据实体中所有的数据分片，而 partitioner 属性则定义了划分数据分片的分区规则，如按哈希取模或是按区间划分等。

不难发现，partitions 和 partitioner 属性刻画的是 RDD 在跨节点方向上的横向扩展，所以我们把它们叫做 RDD 的“横向属性”。

**然后，我们再来说说 dependencies 和 compute 属性。**

在 Spark 中，任何一个 RDD 都不是凭空产生的，每个 RDD 都是基于某种计算逻辑从某个“数据源”转换而来。RDD 的 dependencies 属性记录了生成 RDD 所需的“数据源”，术语叫做父依赖（或父 RDD），compute 方法则封装了从父 RDD 到当前 RDD 转换的计算逻辑。

基于数据源和转换逻辑，无论 RDD 有什么差池（如节点宕机造成部分数据分片丢失），在 dependencies 属性记录的父 RDD 之上，都可以通过执行 compute 封装的计算逻辑再次得到当前的 RDD，这体现RDD的容错性。 

由 dependencies 和 compute 属性提供的容错能力，为 Spark 分布式内存计算的稳定性打下了坚实的基础，这也正是 RDD 命名中 Resilient 的由来。接着观察上图，我们不难发现，不同的 RDD 通过 dependencies 和 compute 属性链接在一起，逐渐向纵深延展，构建了一张越来越深的有向无环图，也就是我们常说的 DAG。

由此可见，dependencies 属性和 compute 属性负责 RDD 在纵深方向上的延展，因此我们不妨把这两个属性称为“纵向属性”。

总的来说，**RDD 的 4 大属性又可以划分为两类，横向属性和纵向属性。其中，横向属性锚定数据分片实体，并规定了数据分片在分布式集群中如何分布；纵向属性用于在纵深方向构建 DAG，通过提供重构 RDD 的容错能力保障内存计算的稳定性。**

