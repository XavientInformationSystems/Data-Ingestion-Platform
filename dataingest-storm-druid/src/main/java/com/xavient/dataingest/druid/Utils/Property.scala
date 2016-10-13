package com.xavient.dataingest.druid.Utils

import java.util.Properties



object  Property {
   val prop = new Properties()
  prop.load( getClass.getResourceAsStream("/application.conf"))
 
  def getProperty(key :String) : String = 
  {
    prop.getProperty(key)
        
  }
  
}