package cn.hecj.spark0318_sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * by hechaojie
  * date 20200318
  * 读取csv文件
  */
object CsvDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("CsvDataSource")
      .master("local[*]")
      .getOrCreate()

    //指定以后读取json类型的数据
    val csv: DataFrame = spark.read.csv("/Users/hecj/Desktop/csv")

    csv.printSchema()

    val pdf: DataFrame = csv.toDF("id", "name", "age")

    pdf.show()

    spark.stop()


  }
}
