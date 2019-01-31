package com.jacle.spark.rdd

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark读取hive的源
  */
object ScalaHiveSource {
  def main(args: Array[String]): Unit = {
    var conf=new SparkConf().setAppName("readHiveSource");
    conf.setMaster("local[*]");

    //设置下并行度，如果中间出现shuffle的情况，可以进行任务数的控制
    conf.set("spark.default.parallelism","50");
    var sc=new SparkContext(conf);


    var hiveContext=new HiveContext(sc);
    hiveContext.sql("use jyqyqb");
    var hiveRdd=hiveContext.sql("select count(1) cout from usrzxzb");
    hiveRdd.foreach(println);


  }
}
