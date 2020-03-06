package cn.hecj.spark0305

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *Spark Streaming接收tcp输入的文本（实时处理）
  */
object TestStreamingWordCountState {

  def main(args: Array[String]): Unit = {
    val sc = new StreamingContext("local[2]","TestStreamingWordCountState",Seconds(10))
    sc.checkpoint("hdfs://localhost:9000/data/demo/state")

    // 监听本地9999端口输入
    var lines = sc.socketTextStream("localhost",9999)
    // 本次wordcount数据
    var parts = lines.flatMap(x=>x.split(" ")).map(x=>(x,1))
    println("-----updateStateByKey------")
    var result = parts.updateStateByKey((values:Seq[Int], state:Option[Int]) =>{
      var newValue = state.getOrElse(0)
      for(value <- values){
        newValue += value
        println("-----values-newValue-----")
        println("value:"+value+",newValue:"+newValue)
      }
      Option(newValue)
    })
    println("-----打印数据------")
    result.print()
    // 开始监听
    sc.start()
    sc.awaitTermination()

  }
}
