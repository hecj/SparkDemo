package cn.hecj.spark0305

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  Spark 32个常用算子总结
  https://blog.csdn.net/fortuna_i/article/details/81170565

  RDD介绍
  https://blog.csdn.net/dsdaasaaa/article/details/94181269

  Spark常用算子
  https://blog.csdn.net/qq_32595075/article/details/79918644

 */
object SparkCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    var dataRDD: RDD[String] = sc.textFile("hdfs://localhost:9000/data/demo/log.txt")

    dataRDD.collect().foreach(println)

    // 每个用户访问次数
    println("--// 每个用户访问次数--")
    var userCountRDD = dataRDD.map(x => {
      (x.split(",").apply(0),1)
    }).reduceByKey((x,y)=>x+y)
    userCountRDD.foreach(println)

    // 每个用户每天的访问次数
    println("--// 每个用户每天的访问次数--")
    var userDayCountRDD = dataRDD.map(x=>{
      var strList = x.split(",")
      (strList.apply(0)+"_"+strList.apply(1),1)
    }).reduceByKey((x,y)=>x+y)
    userDayCountRDD.foreach(println)

    // 每个用户每天 每个接口的访问次数
    println("--// 每个用户每天 每个接口的访问次数--")
    var userDayApiCountRDD = dataRDD.map(x=>{
      var strList = x.split(",")
      (strList.apply(0)+"_"+strList.apply(1)+"_"+strList.apply(3),1)
    }).reduceByKey((x,y)=>x+y)
    userDayApiCountRDD.foreach(println)

    // 每个接口 每天访问次数
    println("--// 每个接口 每天访问次数--")
    var dayApiCountRDD = dataRDD.map(x=>{
      var strList = x.split(",")
      (strList.apply(1)+"_"+strList.apply(3),1)
    }).reduceByKey((x,y)=>x+y)
    dayApiCountRDD.foreach(println)

    dayApiCountRDD.saveAsTextFile("hdfs://localhost:9000/data/demo/dayapi")

  }

}
