package cn.hecj.spark0305

import org.apache.spark.{SparkConf, SparkContext}

/**
  Spark 32个常用算子总结
  https://blog.csdn.net/fortuna_i/article/details/81170565

  RDD介绍
  https://blog.csdn.net/dsdaasaaa/article/details/94181269

 */
object App2  {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    var rdd1 = sc.makeRDD(List(1,2,3))

    // -- collect()	返回 RDD 的所有元素
    rdd1.collect().foreach(println)

    // -- count()	RDD 里元素的个数
    println(rdd1.count())

    // -- countByValue()	各元素在 RDD 中的出现次数
    rdd1.countByValue().foreach(x =>{
      println(x._1+","+x._2)
    })

    // take(num)	从 RDD 中返回 num 个元素
    rdd1.take(2).foreach(println)

    // top(num)	从 RDD 中，按照默认（降序）或者指定的排序返回最前面的 num 个元素
    rdd1.top(3).foreach(println)

    // reduce()	 并行整合所有 RDD 数据，如求和操作
    // reduce将RDD中元素两两传递给输入函数，同时产生一个新的值，新产生的值与RDD中下一个元素再被传递给输入函数直到最后只有一个值为止。
    println(rdd1.reduce((x, y) => {
      // 对于这个x，它代指的是返回值，而y是对rdd各元素的遍历。
//      println(x+","+y)
      x + y
    }))

    // fold(zero)(func)	 和 reduce() 功能一样，但需要提供初始值
    // ### 不理解
    println(rdd1.fold(0)((x, y) => {
            println(x+","+y)
      x + y
    }))

    // foreach(func)	对 RDD 的每个元素都使用特定函数
    rdd1.foreach(println)

    // saveAsTextFile(path)	 将数据集的元素，以文本的形式保存到文件系统中
//    rdd1.saveAsTextFile("hdfs://localhost:9000/data/demo/1.txt");

    // saveAsSequenceFile(path)	将数据集的元素，以顺序文件格式保存到指 定的目录下
//    rdd1.saveAsTextFile("hdfs://localhost:9000/data/demo/test");


    // reduceByKey 顾名思义，reduceByKey就是对元素为KV对的RDD中Key相同的元素的Value进行reduce，因此，
    // Key相同的多个元素的值被reduce为一个值，然后与原RDD中的Key组成一个新的KV对。
    var reduceData = sc.parallelize(List(("A",1),("B",2),("B",4),("C",3)))
    reduceData.reduceByKey((x,y)=>{
      x+y
    }).collect().foreach(println)

    reduceData.map((x)=>(x._1,x._2,x._1+x._2)).foreach(println)


  }

}
