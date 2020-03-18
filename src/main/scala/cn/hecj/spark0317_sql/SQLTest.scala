package cn.hecj.spark0317_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by hechaojie
  * date 201200318
  * * SparkSQL 2.x编程方式
  */
object SQLTest {

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


    val personsRDD : RDD[Row] = lines.map(line =>{
      var fields = line.split(",")
      var id = fields(0).toInt
      var name = fields(1)
      var age = fields(2).toInt
      var score = fields(3).toDouble
      Row(id,name,age,score)
    })
    val schema:StructType = StructType(List(
      StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("score",DoubleType,true)
    ))
    val df:DataFrame = session.createDataFrame(personsRDD,schema);
    import session.implicits._
    val result :Dataset[Row] = df.where($"age" > 20)
    result.show()

    /*
    //将数据进行整理
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Row(id, name, age, fv)
    })

    //结果类型，其实就是表头，用于描述DataFrame
    val schema: StructType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))

    //创建DataFrame
    val df: DataFrame = session.createDataFrame(rowRDD, schema)

    import session.implicits._

    val df2: Dataset[Row] = df.where($"fv" > 98).orderBy($"fv" desc, $"age" asc)

    df2.show()
*/
    session.stop()


  }
}
