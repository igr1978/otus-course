package com.example

import org.apache.log4j.{Level, Logger}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object JsonReader extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  if (args.length != 1) {
    println("Incorrect arguments.")
    println("Usage: /path/to/jar {path/to/json_file.json}")
    sys.exit(-1)
  }

  case class Vine(id: Option[Long], country: Option[String], points: Option[Double], price: Option[Double], title: Option[String], variety: Option[String], winery: Option[String])
  implicit val formats = Serialization.formats(NoTypeHints)

  val sc = new SparkContext(new SparkConf().setAppName("JsonReader").setMaster("local[*]"))

  val filename = args(0)
  //val filename ="winemag.json"

  //Read text file in spark RDD
  val jsonFile = sc.textFile(filename)

  //printing values
  jsonFile.foreach(json =>  println( "decoded json: " + parse(json).extract[Vine]))

  /*for (json <- jsonFile.collect) {
    //println(s"json:\n$json")
    val decodedUser = parse(json).extract[Vine]
    println(s"decoded json: $decodedUser")
  }*/

  println()
  println("finish json decoding.")

  sc.stop()
}
