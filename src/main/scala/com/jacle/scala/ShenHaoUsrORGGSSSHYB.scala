package com.jacle.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object ShenHaoUsrORGGSSSHYB {
  def main(args: Array[String]): Unit = {
     //运行监控的时候，可以看到
     val conf=new SparkConf().setAppName("spark_ShenHaoUsrORGGSSSHYB");
//     conf.setMaster("local");

    //contetxt操作对象
     val sc=new SparkContext(conf);
    //创建rdd1
    val usrORGQYGSXXBLines =sc.textFile("hdfs://m151:8020/data/tyc/shenhao_usrORGQYGSXXB");
    val usrORGQYGSXXBRDD:RDD[(String,String)]=usrORGQYGSXXBLines.map(x=>(x.split("\\^")(0),x.split("\\^")(1)));

    var codeMap=Map[String,String]();
    var file=Source.fromFile("./codeMap.txt")
    for(line <- file.getLines)
    {
        var lineArr=line.split(",");
        codeMap += (lineArr(0)->lineArr(1));
    }
    file.close

    //创建rdd2
     val usrORGGSSSHYBLines=sc.textFile("hdfs://m151:8020/data/tyc/shenhao_usrORGGSSSHYB");
     val usrORGGSSSHYBMapRDD:RDD[(String,String)]=usrORGGSSSHYBLines.map(x=>(x.split("\\^")(0),x.split("\\^")(1)))

    val gxqygxLineRdd =sc.textFile("hdfs://m151:8020/data/tyc/shenhao_usrORGHYQYGXB");
    val gxqygxJoinRdd:RDD[(String,String)]=gxqygxLineRdd.map(x=>(x.split("\\^")(0),x.split("\\^")(1)));

     var joinRdd:RDD[(String,String)]=usrORGQYGSXXBRDD.join(usrORGGSSSHYBMapRDD).map(x=>(x._1,codeMap.get(x._2._2).getOrElse("NotExist")));
     //匹配到的id和编码
     var stage1Rdd:RDD[(String,String)]=joinRdd.filter(x=>x._2!="NotExist").map(x=>(x._1,x._2));
//     _var destRdd:RDD[(String,String)]=stage1Rdd.intersection(gxqygxJoinRdd);

    var vals=stage1Rdd.map(x=>x._1+","+x._2+",天眼查").subtract(gxqygxJoinRdd.map(x=>x._1+","+x._2+",天眼查"));

//    var vals=stage1Rdd.map(x=>x._1+","+x._2+",天眼查").zipWithIndex().subtractByKey(gxqygxJoinRdd.map(x=>x._1+","+x._2+",天眼查").zipWithIndex());
    vals.saveAsTextFile("hdfs://m151:8020/data/tyc/usrORGQYGSXXBRDDJoinedRdd_vals")
//     destJoinRdd.join(gxqygxJoinRdd).filter(x=>x._2._1!="NotExist").filter(x=> x._2._2!="done").map(x=>x._1+","+x._2._1+",天眼查").saveAsTextFile("hdfs://m151:8020/data/tyc/usrORGQYGSXXBRDDJoinedRdd")

     sc.stop();
  }
}
