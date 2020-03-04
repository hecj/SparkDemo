package cn.hecj.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zx on 2017/10/17.
  * 第一个kafka程序
  */
object KafkaWordCountHecj {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

//    val zkQuorum = "node-1:2181,node-2:2181,node-3:2181"
    val zkQuorum = "localhost:2181"
    val groupId = "g1"
    val topic = Map[String, Int]("spark_test" -> 1)

    //创建DStream，需要KafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
    // 取出消息数据 第二个值
    var lines: DStream[String] = data.map((x)=>{x._2})
    // 切分压平
    var words: DStream[String] = lines.flatMap(x =>{x.split(" ")})
    // 单词和1组合
    var wordsOne: DStream[(String,Int)] = words.map(x=> (x,1))
    // 聚合
    var reduce: DStream[(String,Int)] = wordsOne.reduceByKey((x,y)=>x+y)
    // 打印
    reduce.print()
    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()

  }
}
