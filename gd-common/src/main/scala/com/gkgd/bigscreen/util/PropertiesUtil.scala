package com.gkgd.bigscreen.util

import java.io.InputStreamReader
import java.util.Properties
/**
  * @ModelName
  * @Description
  * @Author zhangjinhang
  * @Date 2020/11/19 14:57
  * @Version V1.0.0
  */
object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

}
