package cn.hecj.spark0305

import org.apache.spark.{SparkConf, SparkContext}
/**
  Spark 32个常用算子总结
  https://blog.csdn.net/fortuna_i/article/details/81170565

  RDD介绍
  https://blog.csdn.net/dsdaasaaa/article/details/94181269



 */
object App3  {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    var data = sc.parallelize(1 to 10, 3)

    data.foreach(println)

    var result = data.sample(false,0.2,1)
    result.collect().foreach(println)

    // ------ groupByKey -------
    var data2 = sc.makeRDD(List((1,2),(2,3),(3,5),(2,7)))
    data2.groupByKey().collect().foreach(println)

    // --- sortByKey
    data2.sortByKey().collect().foreach(println)

    // ------ cogroup
    val studentArr = Array((1,"tele"),(2,"yeye"),(3,"wyc"),(3,"hecj "));
    val scoreArr = Array((1,100),(1,200),(2,80),(2,300),(3,100));

    val studentRDD = sc.parallelize(studentArr,1);
    val scoreRDD = sc.parallelize(scoreArr,1);

    val result2 = studentRDD.cogroup(scoreRDD);

    result2.collect().foreach(t=>{
      println("学号:" + t._1);
      println("姓名:" + t._2._1.mkString(" "));
      println("成绩:" + t._2._2.mkString(","));
      println("============");
    })


    studentRDD.countByKey().foreach(println)


  }

}
