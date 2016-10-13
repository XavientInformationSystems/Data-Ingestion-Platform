package com.learning.druidwithstoem

import com.xavient.dataingest.druid.Utils.Property
import com.xavient.dataingest.druid.Utils.Constants




object TestProperty extends App {
  
     
println(Property.getProperty("druid.zk"))

/* val abc =     Property.getProperty("druid.dimensions").split(",").toSeq
    abc.foreach { x => println(x) }

val dimensions = Seq("channel", "cityName", "comment", "countryIsoCode", "countryName", "commentLength", "isAnonymous", "isMinor", "isNew", "isRobot", "isUnpatrolled", "metroCode", "namespace", "page", "regionIsoCode", "regionName", "user", "deltaBucket", "flags", "diffUrl", "added", "delta")
  dimensions.foreach { x => println(x) }*/
  

print(Property.getProperty( Constants.DRUID_DATASOURCE))

}