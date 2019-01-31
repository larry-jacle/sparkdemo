package com.jacle.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Spark读取Json的数据源
  * 1、处理Json数据源的方法1：根据scala的内置json处理,这种适合处理简单的Json类型
  * 2、通过spark json类似spark sql的方式来处理
  */
object ScalaJsonSource2 {
  def main(args: Array[String]): Unit = {
    var conf=new SparkConf().setAppName("readJsonSource");
    conf.setMaster("local[*]");

    //设置下并行度，如果中间出现shuffle的情况，可以进行任务数的控制
    conf.set("spark.default.parallelism","50");
    var sc=new SparkContext(conf);

    //通过变量来引入的时候，一定要是val常量，而不能是变量var
    val sqlContext=new SQLContext(sc);
    import sqlContext.implicits._
//    SQLContext.getOrCreate(sc)


    //直接读取json文件
    val jsonDF=sqlContext.read.json("hdfs://m151:8020/data/hive-data/data.json");
    //dataframe的schema
    println(jsonDF.schema)
    jsonDF.show()
    //方式就是通过json转换为dataframe，从而操作表结构的方式来实现json数据读取
    //通过dataframe创建视图view
//    jsonRdd.createOrReplaceTempView("") //spark2.x的写法
    jsonDF.registerTempTable("tmpJson");
    //将文件转换为DF，通过表格的方式来处理数据
    sqlContext.sql("select * from tmpJson").show

    //RDD转换为DF
    //隐式转换只能处理22列
    var jsonRdd=sc.textFile("hdfs://m151:8020/data/hive-data/data.json");
    //自动转换默认的是将一列直接作为DF的一列
    var jsonDF2=jsonRdd.toDF();
    println(jsonDF2.schema);

    //如果是数组解释为两个col
    var dataRdd2=sc.parallelize(List(("1",List(1,2,3)),("3",List(4,5,6))));
    var jsonDF3=dataRdd2.toDF();
    println(jsonDF3.schema);

    //普通RDD变为DF第一张方式，隐式转换
    //1、var->rdd->DF
    var RddDF=Seq(Person("jacle",23),Person("jacle2",24),Person("jacle3",25),Person("jacle4",26));
    val df3=RddDF.toDF();
    df3.printSchema();
    println("#####--DF3--#####")
    df3.show()

    //json如果要转换为对象，还是要通过读取Rdd，一个个字符串反解为对象

    //2、通过自定义schema来构建DataFrame
    var schema=StructType(List(StructField("name",StringType,true),StructField("nums",IntegerType,false)))
    var selfDF=sqlContext.read.schema(schema).json("hdfs://m151:8020/data/hive-data/data.json");
    selfDF.printSchema();
    selfDF.show();
    //DF的保存结果
//    selfDF.write.save("d:/test_result3.txt")

    //读取嵌套的json
    var dfjson=sqlContext.read.json("hdfs://m151:8020/data/hive-data/test2.json");
    dfjson.registerTempTable("tmpJson");
    var resultDF=sqlContext.sql("select name,address.city,address.state from tmpJson");
    resultDF.show()

    //DataFrame转换为JSon
    resultDF.toJSON.foreach(println);
    //直接可以将DF保存为Json格式
//    resultDF.toJSON.saveAsTextFile("d:/json.txt")
    //生成指定列的DataFrame
    //通过toDF可以更改schema的colname
    var newDF= resultDF.select("name","city").toDF("aname","acity");
    newDF.printSchema();
    newDF.show()


    //使用explode将dataset扁平化
    var scoreJsonDF=sqlContext.read.json("hdfs://m151:8020/data/hive-data/score.json");
    scoreJsonDF.registerTempTable("scoreJson");

    //通过使用sparksql的explode的方法来进行行列转换
    var newScoreDF=scoreJsonDF.select($"name",org.apache.spark.sql.functions.explode($"scores").as("score_alias"));
    newScoreDF.printSchema();
    //只显示前20条
//    newScoreDF.show();
    //boolean参数表示是否显示超过20个字符
    //false表示正常显示
    newScoreDF.drop("name")
    newScoreDF.show(false)

    var distinctJsonDF=sqlContext.read.json("hdfs://m151:8020/data/hive-data/score.txt");
//    distinctJsonDF.distinct().show(false)
    println("-------------")
    distinctJsonDF.dropDuplicates(Seq("name")).show(false)

    //DF的agg聚合方法的使用
    distinctJsonDF.groupBy("name","scores").agg("name"->"count").show(false);

    sc.stop();

  }

  //case一定要定义在方法之外，否则会报错；
  //rdd转换为DF，如果采用case的方法，最多只能转换有22个列的情况；
  case class Person(name: String, age: Int)
}
