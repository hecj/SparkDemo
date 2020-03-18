package cn.hecj.spark0305_rdd

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *Spark Streaming接收tcp输入的文本
  */
object TestStreamingWordCount {

  def main(args: Array[String]): Unit = {
    val sc = new StreamingContext("local[2]","TestStreamingWordCount",Seconds(10))
    // 监听本地9999端口输入
    var lines = sc.socketTextStream("localhost",9999)
    // 将每行的文本 flatMap扁平化分割
    var words = lines.flatMap(x=>x.split(" "))
    // 先进行map变成元祖，然后reduceByKey分组统计
    var wordCounts = words.map(x=>(x,1)).reduceByKey((x,y)=>x+y)
    // 打印统计结果
    wordCounts.print()
    // 开始监听
    sc.start()
    sc.awaitTermination()

  }
}
