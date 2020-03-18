package cn.hecj.spark0309_straming
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * 访问日志统计
  * 日志格式: 时间,ip,菜单,用户id
  * 示例:
  2020-03-09 02:23:45,20.34.209.36,/api/reg,1001
  2020-03-08 02:25:45,21.34.203.31,/api/login,1001
  2020-03-09 03:23:45,23.34.14.84,/api/login,1002

  指标:
  每天每个接口的访问量
  每天每个ip的访问量
  每天每个用户的访问量
  每天每个用户接口的访问量
  *
  * by hechaojie
  * date 20200309
  笔记：
   reduce是一个Action，会把结果返回到Driver端
  */
object AccessCount {
  val group:String = "g1"
  val topic:String = "log2"

  def main(args: Array[String]): Unit = {
    val brokerList = "localhost:9092"
    val zkServer = "localhost:2181"
  // 在本地运行
//    val conf = new SparkConf().setAppName("AccessCount").setMaster("local[2]")
  // 在集群上运行
    val conf = new SparkConf().setAppName("AccessCount")//.setMaster("spark://192.168.30.99:7077")
    val ssc:StreamingContext = new StreamingContext(conf, Duration(5000))
    val topicList: Set[String] = Set(topic)
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    //获取 zookeeper 中的路径 "/access_group/offsets/access_log/"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
    //准备kafka的参数
    val kafkaParams:Map[String, String] = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      //从头开始读取数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )
    val zkClient:ZkClient = new ZkClient(zkServer)
    zkClient.setZkSerializer(ZKStringSerializer)
    var kafkaStream: InputDStream[(String, String)] = buildDirectStream(zkClient,zkTopicPath,ssc,kafkaParams,topicList)


    // kafkaStream.foreachRDD是在Driver端执行的
    kafkaStream.foreachRDD( kafkaRDD =>{
      // 获取当前批次kafka消息的偏移量
      val offsetRanges: Array[OffsetRange] = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      // 每一行的数据
//      val lines : RDD[String] = kafkaRDD.map(_._2)
      // 每一行的数据
      // kafkaRDD.map 调用RDD的算子是在exector端执行的
      val lines: RDD[Array[String]] = kafkaRDD.map(x =>x._2.split(","))

      for(o <- offsetRanges){
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        println("before "+zkPath+":"+o.untilOffset.toString)
      }

      // 每天每个接口的访问量
      CountUtil.dayApiCount(lines)
      // 每天每个ip的访问量
      CountUtil.dayIpCount(lines)
      // 每天每个用户的访问量
      CountUtil.dayUserCount(lines)
      // 每天每个用户接口的访问量
      CountUtil.dayUserApiCount(lines)

      // 更新zk中分区消息的偏移量,无法保证偏移量事物一致,后期优化？？
      for(o <- offsetRanges){
        // zk中分区偏移量路径 如:/access_group/offsets/access_log/0
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        // 更新偏移量
        ZkUtils.updatePersistentPath(zkClient,zkPath,o.untilOffset.toString)
        println(zkPath+":"+o.untilOffset.toString)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 构建直连方式流处理
    */
  def buildDirectStream(zkClient: ZkClient,zkTopicPath:String,ssc:StreamingContext,kafkaParams:Map[String, String],topicList: Set[String]): InputDStream[(String, String)] ={
    val children = zkClient.countChildren(zkTopicPath)
    //如果保存过 offset
    if (children > 0) {
      //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
      var fromOffsets: Map[TopicAndPartition, Long] = Map()
      for (i <- 0 until children) {
        // /g001/offsets/wordcount/0/10001
        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
        // wordcount/0
        val tp = TopicAndPartition(topic, i)
        //将不同 partition 对应的 offset 增加到 fromOffsets 中
        // wordcount/0 -> 10001
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      //Key: kafka的key   values: "hello tom hello jerry"
      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (kafka的key, message) 这样的 tuple
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
      //通过KafkaUtils创建直连的DStream（fromOffsets参数的作用是:按照前面计算好了的偏移量继续消费数据）
      //[String, String, StringDecoder, StringDecoder,     (String, String)]
      //  key    value    key的解码方式   value的解码方式
      return KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      //如果未保存，根据 kafkaParam 的配置使用最新(largest)或者最旧的（smallest） offset
      return KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicList)
    }
  }
}
