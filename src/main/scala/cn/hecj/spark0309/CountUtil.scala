package cn.hecj.spark0309

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

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
        val dateFormat2:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
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

    println("数据条数:"+dayApiRDD.count())

    val dayApiRDDOne: RDD[(String,Int)] = dayApiRDD.map(x=>(x,1))
    val reduceRDD: RDD[(String,Int)] = dayApiRDDOne.reduceByKey((x,y)=>x+y)

//    println("\n===================dayApiCount==============================")
    // 遍历spark分区
    reduceRDD.foreachPartition(partition =>{
      // redis连接应该在Patition上获取
      val redis: Jedis = JedisConnectionPoolKit.getConnection();
      partition.foreach(x => {
        val key = "day_api_count_"+x._1
        redis.incrBy(key,x._2)
        println("after "+key++": "+redis.get(key))
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
        val dateFormat2:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
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
    val dayApiRDDOne: RDD[(String,Int)] = dayApiRDD.map(x=>(x,1))
    val reduceRDD: RDD[(String,Int)] = dayApiRDDOne.reduceByKey((x,y)=>x+y)
//    println("\n===================dayIpCount==============================")
    // 遍历spark分区
    reduceRDD.foreachPartition(partition =>{
      // redis连接应该在Patition上获取
      val redis: Jedis = JedisConnectionPoolKit.getConnection();
      partition.foreach(x => {
        val key = "day_ip_count_"+x._1
        redis.incrBy(key,x._2)
        println("after "+key++": "+redis.get(key))
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
        val dateFormat2:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
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
    val dayApiRDDOne: RDD[(String,Int)] = dayApiRDD.map(x=>(x,1))
    val reduceRDD: RDD[(String,Int)] = dayApiRDDOne.reduceByKey((x,y)=>x+y)
//    println("\n===================dayUserCount==============================")
    // 遍历spark分区
    reduceRDD.foreachPartition(partition =>{
      // redis连接应该在Patition上获取
      val redis: Jedis = JedisConnectionPoolKit.getConnection();
      partition.foreach(x => {
        val key = "day_user_count_"+x._1
        redis.incrBy(key,x._2)
        println("after "+key++": "+redis.get(key))
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
        val dateFormat2:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
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
    val dayApiRDDOne: RDD[(String,Int)] = dayApiRDD.map(x=>(x,1))
    val reduceRDD: RDD[(String,Int)] = dayApiRDDOne.reduceByKey((x,y)=>x+y)
//    println("\n===================dayUserApiCount==============================")
    // 遍历spark分区
    reduceRDD.foreachPartition(partition =>{
      // redis连接应该在Patition上获取
      val redis: Jedis = JedisConnectionPoolKit.getConnection();
      partition.foreach(x => {
        val key = "day_user_api_count_"+x._1
        redis.incrBy(key,x._2)
        println("after "+key++": "+redis.get(key))
      })
      redis.close()
    })
  }
}
