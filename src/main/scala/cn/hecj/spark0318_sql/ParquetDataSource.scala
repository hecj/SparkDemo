package cn.hecj.spark0318_sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * by hechaojie
  * date 20200318
  * 读取parquet文件(推荐这种方式读取文件，更智能)
  */
object ParquetDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ParquetDataSource")
      .master("local[*]")
      .getOrCreate()

    //指定以后读取json类型的数据
    val parquetLine: DataFrame = spark.read.parquet("/Users/hecj/Desktop/parquet")
//    val parquetLine: DataFrame = spark.read.format("parquet").load("/Users/hecj/Desktop/parquet")

    parquetLine.printSchema()

    //show是Action
    parquetLine.show()

    spark.stop()


  }
}
