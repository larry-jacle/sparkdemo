package com.jacle.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.math.BigDecimal


object ShenHaoUsrORGGSSSHYB_Top10 {
  def main(args: Array[String]): Unit = {
     //运行监控的时候，可以看到
     val conf=new SparkConf().setAppName("spark_ShenHaoUsrORGGSSSHYBTOP10");
     conf.set("spark.default.parallelism", "150");
//     conf.setMaster("local");

    //contetxt操作对象
     val sc=new SparkContext(conf);
    //加载工商企业行业关系表
//    val usrORGHYQYGXBLines =sc.textFile("hdfs://m151:8020/data/tyc/shenhao_usrORGHYQYGXB/part-m-00000.snappy");
    val usrORGHYQYGXBLines =sc.textFile("hdfs://m151:8020/data/tyc/shenhao_usrORGHYQYGXB");
    val usrORGHYQYGXBRdd:RDD[(String,String)]=usrORGHYQYGXBLines.map(x=>(x.split("\001")(0),x.split("\001")(1)));


    //加载工商企业
//    val usrORGQYGSXXBLines =sc.textFile("hdfs://m151:8020/data/tyc/shenhao_usrORGQYGSXXB/part-m-00000.snappy");
    val usrORGQYGSXXBLines =sc.textFile("hdfs://m151:8020/data/tyc/shenhao_usrORGQYGSXXB");
    val usrORGQYGSXXBRdd:RDD[(String,(String,String))]=usrORGQYGSXXBLines.map(x=>{
      var lineArr=x.split("\001");
      if(lineArr(1)=="")
      {
          (lineArr(2),(lineArr(0),""))
      }else
      {
          (lineArr(2),(lineArr(0),lineArr(1).substring(0,10)))
      }

    });

    var joinRdd=usrORGHYQYGXBRdd.leftOuterJoin(usrORGQYGSXXBRdd);
    var resultRdd=joinRdd.filter(x=>x._2._2!=None).map(x=>(x._2._1, List((x._1,x._2._2.get._1,x._2._2.get._2)))).reduceByKey(_++_);

    var endRdd=resultRdd.map(x=>{
      var sortedList=x._2.sortBy(y=>(y._2.toDouble,y._3))(Ordering.Tuple2(Ordering.Double.reverse,Ordering.String));
      (x._1,sortedList take 20)
    }).map(x=>{
      var sortedList=x._2
      //检测注册资金是否有相等的情况
      var rankingListBuffer = ListBuffer[(String, String, String,Int)]();
      var lastRaking: Int = 1;

      for (i <- 0 until sortedList.length)
      {
        if (i == 0) {
          rankingListBuffer += ((sortedList(i)._1,sortedList(i)._2,sortedList(i)._3, lastRaking))
        } else {
          //查看是否有相等的
          if (sortedList(i)._2 == sortedList(i - 1)._2) {
            lastRaking = i + 1;
            rankingListBuffer += ((sortedList(i)._1,sortedList(i)._2,sortedList(i)._3, lastRaking))
          } else {
            lastRaking = i + 1;
            rankingListBuffer += ((sortedList(i)._1,sortedList(i)._2,sortedList(i)._3, lastRaking))
          }
        }

       }

      (x._1,rankingListBuffer.toList)
    });
    var end2Rdd=endRdd.flatMap(x=>{
       for(m<-x._2 if m._4<=10) yield x._1+"\001"+m._1+"\001"+m._4
    });

//    endRdd.foreach(println);
    end2Rdd.saveAsTextFile("hdfs://m151:8020/data/tyc/ShenHaoUsrORGGSSSHYB_Top10")

    //关闭资源
    sc.stop();
  }
}
