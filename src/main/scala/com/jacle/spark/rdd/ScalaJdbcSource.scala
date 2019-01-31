package com.jacle.spark.rdd

import java.util.Properties

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark读取Jdbc的数据源
  */
object ScalaJdbcSource {
  def main(args: Array[String]): Unit = {
    var conf=new SparkConf().setAppName("readDbSource");
    conf.setMaster("local[*]");

    //设置下并行度，如果中间出现shuffle的情况，可以进行任务数的控制
    conf.set("spark.default.parallelism","50");
    var sc=new SparkContext(conf);


    var jdbcContext=new SQLContext(sc);
    //获取数据的DataFrame,其实就是一个多为数组，矩阵形式
    var jdbcDF=jdbcContext.read.format("jdbc").options(
      Map("url"->"jdbc:mysql://localhost:3306/test",
      "dbtable"->"(select id,session_id,expires,data from sessions) as some_alias",
      "driver"->"com.mysql.jdbc.Driver",
      "user"-> "root",
      "partitionColumn"->"id",
      "lowerBound"->"0",
      "upperBound"-> "1000",
      "numPartitions"->"2",
      "fetchSize"->"100",
      "password"->"root")).load();

    //jdbcDF其实就是一个RDD
    jdbcDF.foreach(println);
//    jdbcDF.rdd.saveAsTextFile("d:/session.table.txt");

//    jdbcDF.schema
    val url="jdbc:mysql://localhost:3306/test"
    val prop=new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")

    //将数据写入到mysql的数据库相应表中
    //数据不能写入到原来的表格，否则就是清空
    //会自动建立新的表格，存储数据，表格的列就是DataFrame的列
//    jdbcDF.write.mode(SaveMode.Overwrite).jdbc(url,"session",prop)
//    jdbcDF.write.mode(SaveMode.Overwrite).jdbc(url,"session",prop)
//    jdbcDF.write.jdbc(url,"session",prop)

    //这个是新增到表格中
//    org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.saveTable(jdbcDF,url,"sessions",prop)
    println(jdbcDF);
    println(jdbcDF.rdd.count());
    //dataframe的map输入是Row，不能进行底层的类似rdd的map转换
//    jdbcDF.map(x=>)

  }
}
