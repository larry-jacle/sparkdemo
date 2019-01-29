package com.jacle.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * mergeCombiner  Scala合并器
  */
object ScalaCombiner {

  def main(args: Array[String]): Unit = {
    val people = List(("男", "李四"), ("男", "张三"), ("女", "韩梅梅"), ("女", "李思思"), ("男", "马云"));

    //测试中间结果集的combine
    var conf = new SparkConf().setAppName("makeRdd");
    //driver部署是默认的是1
    conf.set("spark.default.parallelism", "50");
    //    conf.set("spark.executor.cores","5")
    //    conf.set("spark.cores.max","25")
    //    conf.setMaster("local[*]");
    conf.setMaster("local")
    var sc = new SparkContext(conf);
    var rdd = sc.parallelize(people, 3);
    //输入x代表的是pairRdd的value
    var combinerRdd = rdd.combineByKey(
      (x: String) => (List(x), 1), //create Combiner
      (x1: (List[String], Int), x: String) => (x1._1 :+ x, x1._2 + 1), //merge value in same partitions
      (x2: (List[String], Int), x3: (List[String], Int)) => (x2._1 ++ x3._1, x2._2 + x3._2) //same partition plus counter
    );
    combinerRdd.foreach(println);


    /*val result = rdd.combineByKey(
       (x:String) => (List(x), 1),  //createCombiner
      (peo: (List[String], Int), x : String) => (x :: peo._1, peo._2 + 1), //mergeValue
      (sex1: (List[String], Int), sex2: (List[String], Int)) => (sex1._1 ::: sex2._1, sex1._2 + sex2._2)) //mergeCombiners
*/

  }

}
