package com.jacle.spark.rdd

import au.com.bytecode.opencsv.CSVReader
import java.io.StringReader

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import au.com.bytecode.opencsv.CSVReader

import scala.collection.JavaConversions

object ShowCsv {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("readJsonSource");
    conf.setMaster("local[*]");

    //设置下并行度，如果中间出现shuffle的情况，可以进行任务数的控制
    //读取文件的是并行度也设置为1，防止读取的时候顺序错乱，如果没有head的可以设置多个并行度
    conf.set("spark.default.parallelism", "1");
    var sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);


    //有换行的csv，databricks还是不支持
    /*    val data =sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true") //这里如果在csv第一行有属性的话，没有就是"false"
      .option("inferSchema","true")//这是自动推断属性列的数据类型。
      .load("d:/hive_csv.csv")//文件的路径

    data.show(false)
    */

    //读取出来就是一个RDD
    /*    val input = sc.textFile("D:/hive_csv.csv")
    val result = input.map(line => {
      val reader = new CSVReader(new StringReader(line));
      reader.readNext()
    }
    );

    result.collect().foreach(x => {x.foreach(println);println("======")})*/

    val input = sc.wholeTextFiles("d:/hive_csv.csv");

    //whileTextFiles返回的就是一个元组，元组只有两列:文件目录、文件内容
   /* for(x<-input)
      {
        println("1",x._1)
        print("2",x._2)
      }*/
    input.flatMap(x=>{
          //需要将java的集合转换为scala的集合
          var csvReader=new CSVReader(new StringReader(x._2));
          var list=csvReader.readAll();
          JavaConversions.asScalaBuffer(list)
      }).foreach(x=>x.foreach(y=>println("content:"+y)));
  }
}

