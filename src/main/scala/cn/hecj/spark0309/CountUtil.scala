package cn.hecj.spark0309

import java.text.{DateFormat, SimpleDateFormat}
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  * 计算工具类
  * 日志格式: 时间,ip,菜单,用户id
  * 2020-03-09 02:23:45,20.34.209.36,/api/reg,1001
  * by hechaojie
  */
object CountUtil {

  /**
    * 每天每个接口的访问量
    * by hechaojie
    */
  def dayApiCount(lines: RDD[Array[String]]): Unit ={

    val dayApiRDD: RDD[String] = lines.map(x=>{
      try {
        val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dateFormat2:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
        val dateStr:String = dateFormat2.format(dateFormat.parse(x(0)))
        (dateStr+"_"+x(2))
      } catch {
        case ex: Exception =>{
          ex.printStackTrace()
          println("Exception "+x(0))
        }
          ("null")
      }
    })

    println("dayApiCount数据条数:"+dayApiRDD.count())

    val dayApiRDDOne: RDD[(String,Int)] = dayApiRDD.map(x=>(x,1))
    val reduceRDD: RDD[(String,Int)] = dayApiRDDOne.reduceByKey((x,y)=>x+y)

    println("dayApiCountreduceRDD数据条数:"+reduceRDD.count())

//    println("\n===================dayApiCount==============================")
    // 遍历spark分区
    reduceRDD.foreachPartition(partition =>{
      // redis连接应该在Patition上获取
      val redis: Jedis = JedisConnectionPoolKit.getConnection()
      partition.foreach(x => {
        val key = "day_api_count_"+x._1
        redis.incrBy(key,x._2)
//        println("dayApiCount after "+key++": "+redis.get(key))
      })
      redis.close()
    })
  }

  /**
    * 每天每个ip的访问量
    * by hechaojie
    */
  def dayIpCount(lines: RDD[Array[String]]): Unit ={

    val dayApiRDD: RDD[String] = lines.map(x=>{
      try {
        val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dateFormat2:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
        val dateStr:String = dateFormat2.format(dateFormat.parse(x(0)))
        (dateStr+"_"+x(1))
      } catch {
        case ex: Exception =>{
          ex.printStackTrace()
          println("Exception "+x(0))
        }
          ("null")
      }

    })
    println("dayIpCount数据条数:"+dayApiRDD.count())
    val dayApiRDDOne: RDD[(String,Int)] = dayApiRDD.map(x=>(x,1))
    val reduceRDD: RDD[(String,Int)] = dayApiRDDOne.reduceByKey((x,y)=>x+y)
    println("dayIpCountRDD数据条数:"+reduceRDD.count())
//    println("\n===================dayIpCount==============================")
    // 遍历spark分区
    reduceRDD.foreachPartition(partition =>{
      // redis连接应该在Patition上获取
      val redis: Jedis = JedisConnectionPoolKit.getConnection();
      partition.foreach(x => {
        val key = "day_ip_count_"+x._1
        redis.incrBy(key,x._2)
//        println("dayIpCount after "+key++": "+redis.get(key))
      })
      redis.close()
    })
  }

  /**
    * 每天每个用户的访问量
    * by hechaojie
    */
  def dayUserCount(lines: RDD[Array[String]]): Unit ={

    val dayApiRDD: RDD[String] = lines.map(x=>{
      try {
        val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dateFormat2:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
        val dateStr:String = dateFormat2.format(dateFormat.parse(x(0)))
        (dateStr+"_"+x(3))
      } catch {
        case ex: Exception =>{
          ex.printStackTrace()
          println("Exception "+x(0))
        }
          ("null")
      }

    })
    println("dayUserCount数据条数:"+dayApiRDD.count())
    val dayApiRDDOne: RDD[(String,Int)] = dayApiRDD.map(x=>(x,1))
    val reduceRDD: RDD[(String,Int)] = dayApiRDDOne.reduceByKey((x,y)=>x+y)
    println("dayUserCountRDD数据条数:"+reduceRDD.count())
//    println("\n===================dayUserCount==============================")
    // 遍历spark分区
    reduceRDD.foreachPartition(partition =>{
      // redis连接应该在Patition上获取
      val redis: Jedis = JedisConnectionPoolKit.getConnection();
      partition.foreach(x => {
        val key = "day_user_count_"+x._1
        redis.incrBy(key,x._2)
//        println("dayUserCount after "+key++": "+redis.get(key))
      })
      redis.close()
    })
  }

  /**
    * 每天每个用户接口的访问量
    * by hechaojie
    */
  def dayUserApiCount(lines: RDD[Array[String]]): Unit ={

    val dayApiRDD: RDD[String] = lines.map(x=>{
      try {
        val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dateFormat2:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
        val dateStr:String = dateFormat2.format(dateFormat.parse(x(0)))
        (dateStr+"_"+x(3)+"_"+x(2))
      } catch {
        case ex: Exception =>{
          ex.printStackTrace()
          println("Exception "+x(0))
        }
          ("null")
      }

    })
    println("dayUserApiCount数据条数:"+dayApiRDD.count())
    val dayApiRDDOne: RDD[(String,Int)] = dayApiRDD.map(x=>(x,1))
    val reduceRDD: RDD[(String,Int)] = dayApiRDDOne.reduceByKey((x,y)=>x+y)
    println("dayUserApiCountRDD数据条数:"+reduceRDD.count())
//    println("\n===================dayUserApiCount==============================")
    // 遍历spark分区
    reduceRDD.foreachPartition(partition =>{
      // redis连接应该在Patition上获取
      val redis: Jedis = JedisConnectionPoolKit.getConnection();
      partition.foreach(x => {
        val key = "day_user_api_count_"+x._1
        redis.incrBy(key,x._2)
//        println("dayUserApiCount after "+key++": "+redis.get(key))
      })
      redis.close()
    })
  }
}
