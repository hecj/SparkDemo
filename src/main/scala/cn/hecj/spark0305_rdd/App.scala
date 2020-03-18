package cn.hecj.spark0305_rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  Spark 32个常用算子总结
  https://blog.csdn.net/fortuna_i/article/details/81170565

  RDD介绍
  https://blog.csdn.net/dsdaasaaa/article/details/94181269

  Spark常用算子
  https://blog.csdn.net/qq_32595075/article/details/79918644

 */
object App {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val sc = new SparkContext(conf)
//    var textIn = sc.textFile("hdfs://localhost:9000/data/demo/core-site.xml")
//
//    // 打印文件内容
//    textIn.foreach(str => {
//      println(str)
//    })
//
//    println(textIn.first());
//
//    textIn.collect().foreach(str =>{
//       println(str)
//    });

//    var rdd1 = sc.makeRDD(List(1,2,3,3))
//    var rdd2 = rdd1.map(x => x+1)
//    rdd2.foreach(x => println(x))
//
//    rdd1.flatMap(x => {
//      x.to(3)
//    })
//
//    rdd1.foreach(println)

    val rdd1=sc.parallelize(Array(("A",1),("B",2),("C",3)))
    rdd1.collect().foreach(println)
    println(rdd1)

    // ------ map 将函数应用于 RDD 的每个元素，返回值是新的 RDD	-----
    var rdd2 = rdd1.map(x =>{x._1+x._2})
    rdd2.collect().foreach(println)
    println(rdd2)

    // ------ flatMap 扁平化 将函数应用于 RDD 的每个元素，将元素数据进行拆分，变成迭代器，返回值是新的 RDD	-----
    var rdd3 = rdd1.flatMap(x => {
      x._1+x._2
    })
    rdd3.collect().foreach(println)
    println(rdd3)

    // ------ filter 函数会过滤掉不符合条件的元素，返回值是新的 RDD	-----
    println("-------filter-----------")
    var rdd4 = rdd1.filter(x =>{
      !x._1.equals("A")
    })
    rdd4.collect().foreach(println)
    println(rdd4)

    // ------ distinct 将 RDD 里的元素进行去重操作	 -----
    println("-------distinct-----------")
    val distinct1=sc.parallelize(Array(("A",1),("A",1),("C",3)))
    var distinctRDD = distinct1.distinct()
    distinctRDD.collect().foreach(println)
    println(distinctRDD)


    // ------ union 生成包含两个 RDD 所有元素的新的 RDD		 -----
    println("-------union-----------")
    var union1 = sc.makeRDD(List(1,2,3,4))
    var union2 = sc.makeRDD(List(3,4,5,6))
    var unionRDD = union1.union(union2)
    unionRDD.collect().foreach(println)
    println(unionRDD)

    // ---intersection()	求出两个 RDD 的共同元素
    var intersectionRDD = union1.intersection(union2);
    intersectionRDD.collect().foreach(println)
    println(intersectionRDD)

    // ---subtract()	将原 RDD 里和参数 RDD 里相同的元素去掉
    var subtractRDD = union1.subtract(union2)
    subtractRDD.collect().foreach(println)
    println(subtractRDD)

    // cartesian()	求两个 RDD 的笛卡儿积
    var cartesianRDD = union1.cartesian(union2)
    cartesianRDD.collect().foreach(println)
    println(cartesianRDD)


  }

}
