package cn.hecj.spark0309_straming

/**
  * Created by zx on 2017/10/20.
  */
import cn.edu360.day10.JedisConnectionPool
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


object JedisConnectionPoolKit{

  val config = new JedisPoolConfig()
  //最大连接数,
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)
  //10000代表超时时间（10秒）
  val pool = new JedisPool(config, "47.104.91.209", 6379, 10000, "redis.xylink.cn@2018")

  def getConnection(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]) {
    val conn = JedisConnectionPool.getConnection()
//    conn.set("income", "1000")
//
//    val r1 = conn.get("xiaoniu")
//
//    println(r1)
//
//    conn.incrBy("xiaoniu", -50)
//
//    val r2 = conn.get("xiaoniu")
//
//    println(r2)
//
//    conn.close()

    val r = conn.keys("*")
    import scala.collection.JavaConversions._
    for (p <- r) {
      println(p + " : " + conn.get(p))
    }
  }

}
