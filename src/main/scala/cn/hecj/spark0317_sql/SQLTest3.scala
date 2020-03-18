package cn.hecj.spark0317_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by hechaojie
  * date 201200318
  * * SparkSQL 2.x编程方式
  */
object SQLTest3 {

  def main(args: Array[String]): Unit = {

    //spark2.x SQL的编程API(SparkSession)
    //是spark2.x SQL执行的入口
    val session = SparkSession.builder()
      .appName("SQLTest1")
      .master("local[*]")
      .getOrCreate()

    /**
      * 示例数据
      1,章节,28,12.3
      2,李思,23,19.3
      3,屋里,19,0.9
      */
    //创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("hdfs://localhost:9000/person")


    val personsRDD : RDD[Person] = lines.map(line =>{
      val fields = line.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val score = fields(3).toDouble
      Person(id,name,age,score)
    })

    val df:DataFrame = session.createDataFrame(personsRDD);
    df.createOrReplaceTempView("person")

    val result : DataFrame= session.sql("select * from person");

    result.show()
    session.stop()


  }


  case class Person(id: Int, name: String, age: Int, score: Double)
}
