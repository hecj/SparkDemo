package cn.hecj.spark0318_sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by hechaojie
  * date 20200318
  */
object JdbcDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JdbcDataSource")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //load这个方法会读取真正mysql的数据吗？
    val logs: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://test.xylink.cn:3306/wx",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "sms_message",
        "user" -> "root",
        "password" -> "xylink@2018")
    ).load()

    logs.printSchema()


//    logs.show()

//    val filtered: Dataset[Row] = logs.filter(r => {
//      r.getAs[Int]("age") <= 13
//    })
//    filtered.show()

    //lambda表达式
    val r: Dataset[Row] = logs.filter($"sms_type" === "register")

//    r.show()

    //val r = logs.where($"age" <= 13)

//    val reslut: DataFrame = r.select($"id")
    val reslut: DataFrame = r.select($"id", $"status", $"sms_type"  as "sms_type")

    // 保存到数据库
//    val props = new Properties()
//    props.put("user","root")
//    props.put("password","xylink@2018")
//    reslut.write.mode("ignore").jdbc("jdbc:mysql://test.xylink.cn:3306/ps", "log1", props)

    //DataFrame保存成text时出错(只能保存一列)
//    reslut.write.text("/Users/hecj/Desktop/text")

//    reslut.write.json("/Users/hecj/Desktop/json")

//    reslut.write.csv("/Users/hecj/Desktop/csv")

    // 写入到hdfs
//    reslut.write.parquet("hdfs://localhost:9000/parquet")
    reslut.write.parquet("/Users/hecj/Desktop/parquet")

    reslut.show()

    spark.close()

  }
}
