package cn.hecj.spark0317_sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * by hechaojie
  * date 20200318
  */
object SQLWordCount {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = SparkSession.builder()
      .appName("SQLWordCount")
      .master("local[*]")
      .getOrCreate()

    //(指定以后从哪里)读数据，是lazy

    //Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    //dataset只有一列，默认这列叫value
    val lines: Dataset[String] = spark.read.textFile("hdfs://localhost:9000/wordcount/input")

    // 导入隐式转换
    import spark.implicits._
    // 切分压平
    val words : Dataset[String] = lines.flatMap(_.split(" "))
    // 注册视图
    words.createTempView("word_count")
    // 执行sql （Transformation，lazy） value是默认的列名
    val result : DataFrame = spark.sql("select value,count(*) c from word_count group by value order by c desc")

    // 执行Action
    result.show()


    // todo 遍历后 写入 mysql redis hbase mongodb 等等
//    result.foreachPartition()

    spark.stop()
  }
}
