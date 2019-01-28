package com.jacle.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object SparkRdd1 {
  def main(args: Array[String]): Unit = {
    //创建Rdd的方法
    var conf=new SparkConf().setAppName("makeRdd");
    //driver部署是默认的是1
//    conf.set("spark.default.parallelism","50");
//    conf.set("spark.executor.cores","5")
//    conf.set("spark.cores.max","25")
//    conf.setMaster("local[*]");
    conf.setMaster("local")
    var sc=new SparkContext(conf);

    //通过文件来创建Rdd
//    val sourceRdd = sc.textFile("hdfs://m151:8020/data/tyc/usrORGDSSJPPXQB");
    //通过变量来创建Rdd
 /*   var rdd_var=sc.makeRDD(List(1,2,3,4,5));
    rdd_var.foreach(println);*/

    //指定分片的个数
    //task可以理解为并行的分片，taskset构成了一个stage
    //分区数的计算：max（excutor个数*core的个数，2）
    //textfile的计算方法：
    //rdd的分区数 = max（本地file的分片数， sc.defaultMinPartitions）
    //rdd的分区数 = max（hdfs文件的block数目， sc.defaultMinPartitions）

    //大量的shuffle也会是的task数量增加
    //parallelize中的partition数量优先级sc.parallelize(List(1,2,3,4,5,6,7),3)> conf.set("spark.default.parallelism","2") > conf.setMaster("local")

/*    val rdd_para = sc.parallelize(List(1, 2, 3, 23))
    rdd_para.foreach(println)
    println(rdd_para.getNumPartitions)*/

    //读取Hdfs文件的时候默认的是1,只有最小分区数大于实际分区数才有效
    //根据平均区分数来计算block的大小
//    val sourceRdd = sc.textFile("hdfs://m151:8020/data/tyc/usrORGDSSJPPXQB");

    //如果不设置spark.default.parallelism，使用的是hdfs的Block数
//    println(sourceRdd.getNumPartitions)
    sc.textFile("file://m151:8020/data/tyc/usrORGDSSJPPXQB")
    println(sc.defaultMinPartitions)
    println(sc.defaultParallelism)


  }
}
