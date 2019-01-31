package com.jacle.spark.rdd

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * DataSet，DataSet包含RDD和DataFrame
  */
object ScalaDataSet {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("readJsonSource");
    conf.setMaster("local[*]");

    //设置下并行度，如果中间出现shuffle的情况，可以进行任务数的控制
    conf.set("spark.default.parallelism", "50");
    var sc = new SparkContext(conf);
    val sqlContext=new SQLContext(sc);

    import sqlContext.implicits._;

    val ds = Seq(Data(1, "one"), Data(2, "two")).toDS()
    ds.collect()
    ds.show()

    //DataSet将json转换为对象
    var StuDS=sqlContext.read.json("hdfs://m151:8020/data/hive-data/data.json").as[Stu];
    StuDS.printSchema();
    StuDS.show();



    sc.stop()
  }

}

case class Data(nums: Int,name: String)
//DataSet的操作，数据精度要选择高精度的类型
case class Stu(name: String,nums:Long)
