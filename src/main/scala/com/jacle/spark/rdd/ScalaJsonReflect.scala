package com.jacle.spark.rdd

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

/**
  * json映射为Scala对象
  */
object ScalaJsonReflect {
  def main(args: Array[String]): Unit = {
    val a =parse(""" {"name":"jacle","age":23} """);

    //返回的是一个jObject
    println(a.toString)
    val b =parse(""" {"name":"jacle","age":23.0} """,true);
    println(b.toString)

    //scala对象变为json字符串有两种方法，一个是dsl
    //下面就是dsl的方式:
    //render将数据类型映射为json的数据类型，compact转换为json字符串
    //("name" -> "joe") 这种形式的jsonobj转换为json字符串
    val c = List(1, 2, 3);
    println(pretty(render(c)))
    println(compact(render(c)))

    val e = ("name" -> "joe")
    val f = compact((render(e)))
    println(f)

    val g = ("name" -> "joe") ~ ("age" -> 35)
    val h = compact(render(g))
    println(h)

    val k = ("name" -> "joe") ~ ("age" -> (None: Option[Int]))
    val l = compact(render(k))
    println(l)

    //1、json字符串变换为对象
    //formats一定要有，否则会报错
    implicit val formats = DefaultFormats
//    implicit  val formats=Serialization.formats(ShortTypeHints(List()));
    //parse只直接将json变为了jsonObject
    var stu=parse(""" {"name":"jacle","age":23,"scores":[{"point":91},{"point":99}]} """).extract[Student];
    //使用逗号分隔，返回的是元组
    println(stu.name,stu.age,stu.scores(0).point)
  }

}

class Student(var name:String,var age:Int,var scores:List[Score]);
class Score(var point:Int)
