package com.jacle.scala

import java.text.SimpleDateFormat
import java.util.Date

import org.joda.time.DateTime

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON


/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
    println("123^456".split("\\^").length)

    var codeMap=Map[String,String]("1"->"2");
    println(codeMap.get("2").getOrElse("None->"));

    var tuple1=(1,2,3)
    test();
  }

  def test()=
  {
    val aMap:mutable.Map[String,Double] = new mutable.HashMap[String, Double]

    val a = Array("A", "B", "C", "D")
    val b = Array(4, 5, 8, 9)

    for(i <- a.indices){
    aMap += (a(i) -> b(i))
  }

    // 从小到大(默认)
    print(aMap)
    print(aMap.toList)
    val mapSortSmall = aMap.toList.sortBy(_._2)
    mapSortSmall.foreach(line => println(line._1 +"\t"+ line._2))

    // 从大到小
    val mapSortBig = aMap.toList.sortBy(-_._2)
    var list1=List(1,2,3);
    var list2=list1:+3
    println(list2)

    var arr=Array((1,2),(2,3));
    arr=arr.:+(2,4)
    println(arr)


    var bMap:mutable.Map[String,Object] = new mutable.HashMap[String, Object]
    var listBufferIn = ListBuffer[((String,String),(String,String))]();

    listBufferIn+=((("1","2"),("1","2")));
    println(listBufferIn.toList)

    var nowDate:Date=new Date();
    var dateFormat=new SimpleDateFormat("YYYYMM");
    println(dateFormat.format(nowDate));


    var jodatime:DateTime=new DateTime();
    var jodatime1=jodatime.minusMonths(1);
    println(jodatime1.toString("YYYYMM"));

    var jodatime2=jodatime.minusMonths(11);
    println(jodatime2.toString("YYYYMM"));

    var tuple1=(1,2,3,4,5);
    println(tuple1.productIterator.mkString(","));

    var array1=Array(1,2,3,4,5);
    println(array1.mkString(","));

    var list3=List(1,2,3,4,5);
    println(list3.mkString(","));

    var jodatime19: DateTime = new DateTime();
    var lastMonthDateTime = jodatime19.minusMonths(1);
    var lastMonth: Int = lastMonthDateTime.toString("YYYYMM").toInt;
    var beginMonth = jodatime19.minusMonths(12).toString("YYYYMM").toInt;

    println(jodatime19)
    println(beginMonth)
    println(lastMonth)

    var some1=Some((1,2))
    println(some1.get._1)

    var lista=List('a','b','c','d','e')
    println(lista take 20);

    for(i<-Range(0,lista.length,2))
      {
        println("current step is:"+lista(i));
      }

    for(i<-0.to(lista.length,2))
      {
        println(lista(i));
      }

    //JSON转换的时候一定要是标准的json，内部使用双引号
    var jsonStr="{\"name\":\"jacle\",\"age\":23}";
    println(JSON.parseFull(jsonStr));

    val xs = Map("a" -> List(11,111,22), "b" -> List(22,222,2222)).flatMap(_._2)
    println(xs.mkString(","))

    //flatMap返回的值跟主类型一致
    val xs2 = Map("a" -> List(11,111,22), "b" -> List(22,222,2222)).flatMap(x=>{println(x);x._2})
    println("1L",xs2)

    //flatmap输出的一定是一个List，将各个list进行合并
    //flatMap返回的的类型跟输入样
    val ys = Map("a" -> List(1 -> 11,1 -> 111), "b" -> List(2 -> 22,2 -> 222)).flatMap(x=>{println(x._2);x._2})
    println("2L",ys)

    //1->11可以理解一个key-value的tuple
    var listMap=Map(1 -> 11,1 -> 111)++Map(2 -> 11,2 -> 111);
    println(listMap);

    var m1=1->11;
    println(m1);


  }

}
