package com.jacle.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRdd2 {
  def main(args: Array[String]): Unit = {
    //创建Rdd的方法
    var conf=new SparkConf().setAppName("makeRdd2");
    //driver部署是默认的是1
    conf.set("spark.default.parallelism","10");
    conf.setMaster("local")
    var sc=new SparkContext(conf);


    var dataList=List((1,2) , (3,4) , (3,6) );
    var rdd=sc.parallelize(dataList)

    //对kv中v的transform
    rdd.mapValues(x=>x+1).foreach(println)

    //返回的是range，1 到 5 ，until不包含最后一个
//    List(1, 2, 3, 4, 5)
//    List(1, 2, 3, 4)
    println((1 to 5).toList)
    println((1 until 5).toList)
    println(Range(1,5))


    //第二个参数表示分区数
    val a = sc.parallelize(1 to 4, 2)
    a.foreach(println)

    //flatmap
    var rdd3=sc.parallelize(Array(2,3,4))
    //flatMap返回的是List或者数组，每个元素是单的一个rdd元素
    rdd3.flatMap(x=>Array((x, x), (x, x))).foreach(println)

    //flatMapValues
    var rdd4=sc.parallelize(Array((1,(1,2,3,4))))
    //函数返回的是一个数组
    rdd4.flatMapValues(x=>x.productIterator.toList).foreach(println);

    //mapPartitions
    val a2= sc.parallelize(90 to 100, 5)
    def sumOfEveryPartition(input: Iterator[Int]): Int = {
      var total = 0
      input.foreach { elem =>
        total += elem
      }
      total
    }
    //mapPartition返回的是一个Iterator
    a2.mapPartitions(x=>Iterator({var total=0; var tmp=0;x.foreach(e=>{tmp=e;total=total+e}); (tmp,total)})).foreach(println);

  }
}
