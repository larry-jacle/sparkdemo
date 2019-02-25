package com.jacle.parquet

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * spark使用scala进行parquet文件读取
  * spark天然的支持parquet文件
  */
object SparkParquet
{
  def main(args: Array[String]): Unit =
  {
    var conf = new SparkConf().setAppName("sparkparquet");
    conf.setMaster("local[*]");

    //设置下并行度，如果中间出现shuffle的情况，可以进行任务数的控制
    conf.set("spark.default.parallelism", "50");
    var sc = new SparkContext(conf);
    val session=new SQLContext(sc);

    session.sparkContext.setLogLevel("WARN");
    var path="hdfs://m201:8020/user/hive/warehouse/t_parquet";
    var df=session.read.parquet(path);

    df.show(10,false);
    //df写入到本地
    df.write.mode(SaveMode.Overwrite ).parquet("d:/save.parquet");
  }

}
