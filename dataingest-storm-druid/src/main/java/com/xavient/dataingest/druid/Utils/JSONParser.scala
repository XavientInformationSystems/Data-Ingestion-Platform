package com.xavient.dataingest.druid.Utils

import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper
import scala.collection.JavaConversions._
import java.util.ArrayList
import collection.mutable._

object JSonParser {
  val m = new ObjectMapper()

  val jsonAttributes: List[String] =  Property.getProperty(Constants.DRUID_DIMENSIONS).split(",").toList

  def main(args: Array[String]): Unit = {

   val a = JSonParser.parse("{ \"timestamp\": \"2016-10-12T09:58:45Z\", \"id\": 784035014430167040, \"text\": \"Mucho interés por el Big data, pero cada vez hay menos empresas invirtiendo en ello\", \"source\": \"hello\", \"reTweeted\": false, \"username\": \"Anderson\", \"createdAt\": \"Oct 6, 2016 7:48:08 PM\", \"retweetCount\": 0, \"userLocation\": \"unknown\", \"inReplyToUserId\": -1, \"inReplyToStatusId\": -1, \"userScreenName\": \"alvinscastro\", \"userDescription\": \"100% Humano sin extremos ideológicos. Libertad sobre todas las cosas\", \"userFriendsCount\": 6148, \"userStatusesCount\": 5125, \"userFollowersCount\": 5612 }")
    
    a.foreach(f=> f.asInstanceOf[Map[String,Any]].foreach(g=> println(g._1 + g._2)))

  }
  
  


  def parse(str: String):java.util.List[java.lang.Object] = {
 
    val rootNode = m.readTree(str)
  val a=   getMetadata(rootNode)
  
    var returnList: java.util.List[java.lang.Object]  =  List[java.lang.Object](a)
    return returnList
  }

  def getMetadata(node: JsonNode): Map[String, Any] = {
    println("here")
    val values = new ArrayList[Any](jsonAttributes.length)
    val returnList: collection.mutable.Map[String, Any]  = collection.mutable.Map[String, Any]()
      jsonAttributes.foreach { attribute => 
      var value = node.path(attribute).getValueAsText
   
   
      returnList += attribute -> value

    }
    println(returnList.size)
    
    
    returnList

  }
}