package com.example

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object JsonReader extends App {
  case class Vine(id: Option[Long], country: Option[String], points: Option[Double], title: Option[String], variety: Option[String], winery: Option[String])
  implicit val formats = Serialization.formats(NoTypeHints)

 /* case class Vine(
                   id: Option[Long] = None,
                   country: Option[String] = None,
                   points: Option[Double] = None,
                   title: Option[String] = None,
                   variety: Option[String] = None,
                   winery: Option[String] = None
                 )*/

  val sc = new SparkContext(new SparkConf().setAppName("JsonReader").setMaster("local[*]"))

  val filename = args(0)
  //val filename ="winemag.json"

  //Read text file in spark RDD
  val jsonFile = sc.textFile(filename)

  //printing values
  for (json <- jsonFile.collect) {
    //println(s"json:\n$json")
    val decodedUser = parse(json).extract[Vine]
    println(s"decoded json: $decodedUser")
  }
  println()
  println(s"finish json decoding.")
}
