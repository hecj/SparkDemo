package cn.hecj.spark0318_sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * by hechaojie
  * date 20200218
  * 读json数据源
  */
object JsonDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JdbcDataSource")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //指定以后读取json类型的数据(有表头)
    val jsons: DataFrame = spark.read.json("/Users/hecj/Desktop/json")

    val filtered: DataFrame = jsons.where($"status" === 1)

    filtered.printSchema()

    filtered.show()

    spark.stop()


  }
}
