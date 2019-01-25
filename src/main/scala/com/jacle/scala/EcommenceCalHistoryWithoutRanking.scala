package com.jacle.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal

/**
  * 电商详情计算
  */
object EcommenceCalHistoryWithoutRanking {
  def main(args: Array[String]): Unit = {

    if(args.length==0)
    {
      println("请输入参数，当前月份，例如输入:201901,计算的是201812月份的数据");
      return ;
    }

    val conf = new SparkConf().setAppName("spark-ecnomic");
    //本机测试是打开，服务器上关闭此选项
    //    conf.setMaster("local");

    //contetxt操作对象
    val sc = new SparkContext(conf);
    //    val sourceRdd = sc.textFile("hdfs://m151:8020/data/tyc/usrORGDSSJPPXQB/part-m-00009");
    val sourceRdd = sc.textFile("hdfs://m151:8020/data/tyc/usrORGDSSJPPXQB");



    //计算当前的月份，获取上个月的月份，日期格式为YYYYMM
    var format=DateTimeFormat.forPattern("YYYYMM")
    var jodatime: DateTime = DateTime.parse(args(0),format);
    var lastMonth: Int = jodatime.minusMonths(1).toString("YYYYMM").toInt;
    var lastTwoMonth: Int = jodatime.minusMonths(2).toString("YYYYMM").toInt;

    //开始时间为当前月，往前推12个月
    var beginMonth = jodatime.minusMonths(12).toString("YYYYMM").toInt;


    //源rdd进行转换
    //显示的数据集为ID、LMJB、LMDM、SJ、PPINBBM、XL、XSE
    var rdd1: RDD[(String, String, String, String, String, String, String)] = sourceRdd.map(x => {
      var rowArr = x.split("\001");
      (rowArr(0), rowArr(2), rowArr(3), rowArr(5), rowArr(6), rowArr(7), rowArr(8));
    });

    //1、过滤指定日期之外的数据
    rdd1 = rdd1.filter(x => Integer.parseInt(x._4) >= beginMonth && Integer.parseInt(x._4) <= lastMonth);
    //2、计算总销量
    var rdd2: RDD[(String, BigDecimal)] = rdd1.map(x => (x._2 + "," + x._3 + "," + x._5, BigDecimal(x._7))).reduceByKey(_ + _);
    var rdd3 = rdd2.map(x => {
      var arr = x._1.split(",");
      ((arr(0), arr(1)), (arr(2), x._2))
    })
    //3、计算排名
 /*   var lastMonthRaningRdd = sc.textFile("hdfs://m151:8020/data/tyc/lastRanking_" + lastTwoMonth).map(x => {
      var arr = x.split("\001");
      ((arr(1), arr(2), arr(3)), arr(4))
    });*/
    var rankingRdd = rdd3.groupByKey().map(x => {
      var m1 = x._2.toList.sortBy(n => n._2)(Ordering.BigDecimal.reverse)
      (x._1, m1)
    }).flatMap(x => {
      //解决如果销量相同的情况
      //保存销量相等的销售额
      var rankingListBuffer = ListBuffer[((String, String, String), (BigDecimal, Int))]();
      var lastRaking: Int = 1;

      for (i <- 0 until x._2.length) {
        if (i == 0) {
          rankingListBuffer += (((x._1._1, x._1._2, x._2(i)._1), (x._2(i)._2, lastRaking)))
        } else {
          //查看是否有相等的
          if (x._2(i)._2 == x._2(i - 1)._2) {
            rankingListBuffer += (((x._1._1, x._1._2, x._2(i)._1), (x._2(i)._2, lastRaking)))
          } else {
            lastRaking = i + 1;
            rankingListBuffer += (((x._1._1, x._1._2, x._2(i)._1), (x._2(i)._2, lastRaking)))
          }
        }
      }
      rankingListBuffer.toList
    });

    var rankingMapRdd = rankingRdd.map(x => {
      var rankingStep = 0;
      (x._1, (x._2._1, x._2._2, 0))
    })

    //4、计算标记点(两类标记点：销量和销售额)
    //第二次计算的时候要注意下开始点是否是标记点
    //先获取所有点的数据(行业id，行业代码，品牌id，月份，销量)
    //显示的数据集为ID、LMJB、LMDM、SJ、PPINBBM、XL、XSE
    //这里的reducebykey是将key相同的多个数值合并到一个map里面，进而转换为list
    var xlRdd = rdd1.map(x => ((x._2, x._3, x._5), Map(x._4.toInt -> x._6))).reduceByKey(_ ++ _);
    var xseRdd = rdd1.map(x => ((x._2, x._3, x._5), Map(x._4.toInt -> x._7))).reduceByKey(_ ++ _);

    var flagPointThreshold: Float = 0.2f;
    var xlFlagPointRdd = xlRdd.map(x => (x._1, x._2.toList.sortBy(_._1))).map(x => {
      //计算标记点
      if (x._2.length == 1) {
        (x._1, "[]")
      } else {
        var flagPoint = x._2(0)
        var flagPointList = new scala.collection.mutable.ListBuffer[(String, Float, Int)]
        for (i <- 0 until x._2.length) {
          if (math.abs((x._2(i)._2.toFloat - flagPoint._2.toFloat) / x._2(i)._2.toFloat) > flagPointThreshold) {
            flagPointList += ((x._2(i)._1.toString, x._2(i)._2.toFloat, 1));
            flagPoint = x._2(i);
          } else {
            flagPointList += ((x._2(i)._1.toString, x._2(i)._2.toFloat, 0));
          }
        }
        var jsonBuffer: StringBuffer = new StringBuffer();
        jsonBuffer.append("[");
        for (i <- 0 until flagPointList.length) {
          if (i > 0) {
            jsonBuffer.append(",{'publishdate':" + flagPointList(i)._1 + ",'val':" + flagPointList(i)._2 + ",'mark':" + flagPointList(i)._3 + "}")
          } else {
            jsonBuffer.append("{'publishdate':" + flagPointList(i)._1 + ",'val':" + flagPointList(i)._2 + ",'mark':" + flagPointList(i)._3 + "}")
          }
        }
        jsonBuffer.append("]");

        (x._1, jsonBuffer.toString)
      }
    });

    var xseFlagPointRdd = xseRdd.map(x => (x._1, x._2.toList.sortBy(_._1))).map(x => {
      //计算标记点
      if (x._2.length == 1) {
        (x._1, "[]")
      } else {
        var flagPoint = x._2(0)
        var flagPointList = new scala.collection.mutable.ListBuffer[(String, Float, Int)]
        for (i <- 0 until x._2.length) {
          if (math.abs((x._2(i)._2.toFloat - flagPoint._2.toFloat) / x._2(i)._2.toFloat) > flagPointThreshold) {
            flagPointList += ((x._2(i)._1.toString, x._2(i)._2.toFloat, 1));
            flagPoint = x._2(i);
          } else {
            flagPointList += ((x._2(i)._1.toString, x._2(i)._2.toFloat, 0));
          }
        }
        var jsonBuffer: StringBuffer = new StringBuffer();
        jsonBuffer.append("[");
        for (i <- 0 until flagPointList.length) {
          if (i > 0) {
            jsonBuffer.append(",{'publishdate':" + flagPointList(i)._1 + ",'val':" + flagPointList(i)._2 + ",'mark':" + flagPointList(i)._3 + "}")
          } else {
            jsonBuffer.append("{'publishdate':" + flagPointList(i)._1 + ",'val':" + flagPointList(i)._2 + ",'mark':" + flagPointList(i)._3 + "}")
          }
        }
        jsonBuffer.append("]");

        (x._1, jsonBuffer.toString)
      }
    });

    //获取上个月的销量和销售额
    //显示的数据集为ID、LMJB、LMDM、SJ、PPINBBM、XL、XSE
    var lastMonthXSEXL = rdd1.filter(x => Integer.parseInt(x._4) == lastMonth).map(x => ((x._2, x._3, x._5), (x._6, x._7))).distinct();
    var last_resultRdd = lastMonthXSEXL.join(rankingMapRdd).join(xseFlagPointRdd).join(xlFlagPointRdd);
    var splitStr: String = "\001";
    var outputRdd = last_resultRdd.map(x => x._1.productIterator.mkString(splitStr) + splitStr + x._2._1._1._1.productIterator.mkString(splitStr) + splitStr + x._2._1._1._2.productIterator.mkString(splitStr) + splitStr + "{'xse_Points':" + x._2._1._2 + ",'xl_Points':" + x._2._2 + "}" + splitStr + lastMonth + splitStr + "淘数据" + splitStr + "天猫")

//    outputRdd.foreach(println);
        outputRdd.saveAsTextFile("hdfs://m151:8020/data/tyc/guoyuxiang_calEcnomic");
    //    xseRdd.map(x=>(x._1,x._2.toList.sortBy(_._1))).foreach(println)
    sc.stop();

  }
}
