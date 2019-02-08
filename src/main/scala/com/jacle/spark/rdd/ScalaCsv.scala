package com.jacle.spark.rdd

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * scala读取csv文件
  */
object ScalaCsv {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("readJsonSource");
    conf.setMaster("local[*]");

    //设置下并行度，如果中间出现shuffle的情况，可以进行任务数的控制
    //读取文件的是并行度也设置为1，防止读取的时候顺序错乱，如果没有head的可以设置多个并行度
    conf.set("spark.default.parallelism", "1");
    var sc = new SparkContext(conf);

    //当单元格没有换行符的情况，可以进行类似的文件读取
    var csvFile = sc.textFile("d:/csv_hive.csv")
    var lines = csvFile.map(line => {
      val reader = new CSVReader(new StringReader(line));
      reader.readNext()
    });

    for (data <- lines) {
      for (unit <- data) {
        println(unit)
      }
    }

    //DF的方式读取
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._

    //spark2.2+的处理方式
    /*    val data = sqlContext.read.option("header", "true").option("inferSchema", "true")
      .option("multiLine", "true").option("delimiter", ",")
      .format("csv")
      .load("D:/csv_hive3.csv")
    data.show(false)*/

    //wholeTextFiles  将记录转换为了k、v的格式
    //一个文件的内容，在一个元组内
    val input = sc.wholeTextFiles("D:/csv_hive3.csv")

    for (res <- input) {
      println(res)
    }


    //元组进行flatmap
    val arr=sc.parallelize(Array(("A",1),("B",2),("C",3)))
    //加不加括号都一样
    arr.flatMap(x=>x._1+x._2).foreach(println)

    val xs = Map("a" -> List(11,111,22), "b" -> List(22,222,2222)).flatMap(_._2)
    println(xs.mkString(","))

  }

}
