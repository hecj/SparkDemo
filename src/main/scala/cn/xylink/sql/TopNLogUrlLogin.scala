package cn.xylink.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by hechaojie
  * date 20200422
  * * SparkSQL 2.x编程方式
  * 指标1.统计登录频率topN
  hdfs路径：http://test.xylink.cn:50070/explorer.html#/data/www/log/log_url_login
  数据格式：uuid,token,user_id,phone,remote_ip,device_id,sys_version,version,url,create_at,device_name,version_name,
  数据示例：7ca06341fb7e458d82d7c85061f17de7,appabd0af5d1b7a4e4b9207d529d0bae33d,20190802154933473002,15811372713,117.136.0.212
,D47A87DB-6CEE-49E1-BFBE-A90ABC1897B3_D9605CB3-9731-4234-86A2-B61E82E19FB2,iOS13.4.1,1,/api/sct/v2/article/unread/list,1587542326776,iPhone 8,1.0,
  */
object TopNLogUrlLogin {

  def main(args: Array[String]): Unit = {

    //spark2.x SQL的编程API(SparkSession)
    //是spark2.x SQL执行的入口
    val session = SparkSession.builder()
      .appName("TopNLogUrlLogin")
      .master("local[*]")
      .getOrCreate()

    // 提交到生产环境 去掉 .master("local[*]")
//    val session = SparkSession.builder()
//      .appName("TopNLogUrlLogin")
//      .getOrCreate()

    // 多个文件
    var fileList = Array(
      "hdfs://test.xylink.cn:19000/data/www/log/log_url_login/20200422",
      "hdfs://test.xylink.cn:19000/data/www/log/log_url_login/20200423",
      "hdfs://test.xylink.cn:19000/data/www/log/log_url_login/20200424");
    val lines: RDD[String] = session.sparkContext.textFile(fileList.mkString(","))

    //创建RDD
//    val lines: RDD[String] = session.sparkContext.textFile("hdfs://test.xylink.cn:19000/data/www/log/log_url_login/20200424")

    // 整理字段
    val logsRDD : RDD[LogUrl] = lines.map(line =>{
      val fields = line.split(",")
      val uuid = fields(0)
      val token = fields(1)
      val user_id = fields(2)
      val phone = fields(3)
      val remote_ip = fields(4)
      val device_id = fields(5)
      val sys_version = fields(6)
      val version = fields(7)
      val url = fields(8)
      val create_at = fields(9).toLong
      val device_name = fields(10)
      val version_name = fields(11)
      LogUrl(uuid,token,user_id,phone,remote_ip,device_id,sys_version,version,url,create_at,device_name,version_name)
    })

    // 映射表
    val df:DataFrame = session.createDataFrame(logsRDD);
    df.createOrReplaceTempView("log_url")

    // 统计 /api/user/common/v2/token/check

    // url访问排行(登录用户排行)
    val result : DataFrame= session.sql("select phone,count(1) c from log_url t where t.url='/api/user/common/v2/token/check' group by phone order by c desc limit 100");

    // 1.使用spark sql统计活跃用户topN
    //val result : DataFrame= session.sql("select phone,count(1) c from log_url group by phone order by c desc limit 100");


    result.show()
    session.stop()

  }

  case class LogUrl(uuid: String,token: String,user_id: String,phone: String,remote_ip: String,device_id: String
                    ,sys_version: String,version: String,url: String,create_at: Long,device_name: String,version_name: String)
}
