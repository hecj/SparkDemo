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

  val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val dateFormat2:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  /**
    * 每天每个接口的访问量
    * by hechaojie
    */
  def dayApiCount(lines: RDD[Array[String]]): Unit ={

    val dayApiRDD: RDD[String] = lines.map(x=>{
      try {
        val dateStr:String = dateFormat2.format(dateFormat.parse(x(0)))
        (dateStr+"_"+x(2))
      } catch {
        case ex: Exception =>{
          println("IO Exception")
        }
          ("null")
      }

    })
    val dayApiRDDOne: RDD[(String,Int)] = dayApiRDD.map(x=>(x,1))
    val reduceRDD: RDD[(String,Int)] = dayApiRDDOne.reduceByKey((x,y)=>x+y)

    reduceRDD.foreach(println)

    println("\n===================dayApiCount==============================")
    // 遍历spark分区
    reduceRDD.foreachPartition(partition =>{
      println("----------------------")
      println("分区:"+partition)
      // redis连接应该在Patition上获取
      val redis: Jedis = JedisConnectionPoolKit.getConnection();
      println("redis:"+redis)
      partition.foreach(x => {
        println("before "+redis.get("day_api_count_"+x._1))
        println("day_api_count_"+x._1+":"+x._2)
        redis.incrBy("day_api_count_"+x._1,x._2)
        println("after "+redis.get("day_api_count_"+x._1))
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
        val dateStr:String = dateFormat2.format(dateFormat.parse(x(0)))
        (dateStr+"_"+x(1))
      } catch {
        case ex: Exception =>{
          println("IO Exception")
        }
          ("null")
      }

    })
    val dayApiRDDOne: RDD[(String,Int)] = dayApiRDD.map(x=>(x,1))
    val reduceRDD: RDD[(String,Int)] = dayApiRDDOne.reduceByKey((x,y)=>x+y)
    println("\n===================dayIpCount==============================")
    // 遍历spark分区
    reduceRDD.foreachPartition(partition =>{
      println("----------------------")
      println("分区:"+partition)
      // redis连接应该在Patition上获取
      val redis: Jedis = JedisConnectionPoolKit.getConnection();
      println("redis:"+redis)
      partition.foreach(x => {
        println("before "+redis.get("day_ip_count_"+x._1))
        println("day_ip_count_"+x._1+":"+x._2)
        redis.incrBy("day_ip_count_"+x._1,x._2)
        println("after "+redis.get("day_ip_count_"+x._1))
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
        val dateStr:String = dateFormat2.format(dateFormat.parse(x(0)))
        (dateStr+"_"+x(3))
      } catch {
        case ex: Exception =>{
          println("IO Exception")
        }
          ("null")
      }

    })
    val dayApiRDDOne: RDD[(String,Int)] = dayApiRDD.map(x=>(x,1))
    val reduceRDD: RDD[(String,Int)] = dayApiRDDOne.reduceByKey((x,y)=>x+y)
    println("\n===================dayUserCount==============================")
    // 遍历spark分区
    reduceRDD.foreachPartition(partition =>{
      println("----------------------")
      println("分区:"+partition)
      // redis连接应该在Patition上获取
      val redis: Jedis = JedisConnectionPoolKit.getConnection();
      println("redis:"+redis)
      partition.foreach(x => {
        println("before "+redis.get("day_user_count_"+x._1))
        println("day_user_count_"+x._1+":"+x._2)
        redis.incrBy("day_user_count_"+x._1,x._2)
        println("after "+redis.get("day_user_count_"+x._1))
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
        val dateStr:String = dateFormat2.format(dateFormat.parse(x(0)))
        (dateStr+"_"+x(3)+"_"+x(2))
      } catch {
        case ex: Exception =>{
          println("IO Exception")
        }
          ("null")
      }

    })
    val dayApiRDDOne: RDD[(String,Int)] = dayApiRDD.map(x=>(x,1))
    val reduceRDD: RDD[(String,Int)] = dayApiRDDOne.reduceByKey((x,y)=>x+y)
    println("\n===================dayUserApiCount==============================")
    // 遍历spark分区
    reduceRDD.foreachPartition(partition =>{
      println("----------------------")
      println("分区:"+partition)
      // redis连接应该在Patition上获取
      val redis: Jedis = JedisConnectionPoolKit.getConnection();
      println("redis:"+redis)
      partition.foreach(x => {
        println("before "+redis.get("day_user_api_count_"+x._1))
        println("day_user_api_count_"+x._1+":"+x._2)
        redis.incrBy("day_user_api_count_"+x._1,x._2)
        println("after "+redis.get("day_user_api_count_"+x._1))
      })
      redis.close()
    })
  }
}
