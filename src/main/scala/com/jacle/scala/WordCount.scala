package com.jacle.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
     //运行监控的时候，可以看到
     val conf=new SparkConf().setAppName("spark1");
     conf.setMaster("local");

    //contetxt操作对象
     val sc=new SparkContext(conf);
/*     val lines=sc.textFile("hdfs://m151:8020/spark-data-tset/test.txt");

     val flatmaprows=lines.flatMap(_.split(" "));
     val maps=flatmaprows.map((_,1));
     val result=maps.reduceByKey(_+_);*/


    val arr=sc.parallelize(Array(("A",1,5),("B",2,6),("C",3,7)))
    arr.flatMap(x=>(x._1+x._2+x._3)).foreach(println)

/*
    result.map(x=>{
       val m=x._1+"c";
      (m,x._2+"map方法块")
    }).foreach(map => println(map._1 +":"+ map._2))
*/

     sc.stop();

  }
}
