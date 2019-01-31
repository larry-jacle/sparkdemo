package com.jacle.spark.rdd

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json.JSON

/**
  * Spark读取Json的数据源
  * 1、处理Json数据源的方法1：根据scala的内置json处理,这种适合处理简单的Json类型
  * 2、通过spark json类似spark sql的方式来处理
  */
object ScalaJsonSource {
  def main(args: Array[String]): Unit = {
    var conf=new SparkConf().setAppName("readJsonSource");
    conf.setMaster("local[*]");

    //设置下并行度，如果中间出现shuffle的情况，可以进行任务数的控制
    conf.set("spark.default.parallelism","50");
    var sc=new SparkContext(conf);
    var jsonStr="{\"name\":\"jacle\",\"age\":23,\"person\":{\"name\":\"jacle\",\"age\":23}}";

    //parallelize 一定要放入数组等集合，如果放置字符串会变为Char数组
    var jsonRdd=sc.parallelize(List(jsonStr.toString));
    jsonRdd.map(x=>JSON.parseFull(x)).foreach(m=>{
      m match
      {
          //case Some的时候，显示具体的内容
        case Some(map:Map[String,Any])=>println(map)
        case None=>println(None);
        case _=>println("_");
      }
    });






  }
}
