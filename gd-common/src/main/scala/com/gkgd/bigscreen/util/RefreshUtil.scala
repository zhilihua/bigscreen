package com.gkgd.bigscreen.util

import java.util.Properties

import com.gkgd.bigscreen.constant.ConfigConstant
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}

/**
  * @ModelName
  * @Description
  * @Author zhangjinhang
  * @Date 2020/12/22 10:10
  * @Version V1.0.0
  */
object RefreshUtil {
  val properties: Properties = PropertiesUtil.load("config.properties")

  val httpclient: CloseableHttpClient = HttpClients.createDefault

  def refreshBuildDisposal(): Unit = {
//    val refreshUrl = "http://42.236.61.123:8802/data/flush/build"
    val refreshUrl = properties.getProperty("interface.build.disposal.amount")
    try {
      val httpGet = new HttpGet(refreshUrl)
//      import org.apache.http.client.config.RequestConfig
//      val requestConfig: RequestConfig = RequestConfig.custom.setConnectTimeout(1000).setConnectionRequestTimeout(1000).setSocketTimeout(1000).build
//      httpGet.setConfig(requestConfig)

      val httpResponse: CloseableHttpResponse = httpclient.execute(httpGet)
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }
  def refreshCreditRank(): Unit = {
    val refreshUrl = properties.getProperty("interface.credit")
    try {
      val httpGet = new HttpGet(refreshUrl)
//      import org.apache.http.client.config.RequestConfig
//      val requestConfig: RequestConfig = RequestConfig.custom.setConnectTimeout(1000).setConnectionRequestTimeout(1000).setSocketTimeout(1000).build
//      httpGet.setConfig(requestConfig)

      val httpResponse: CloseableHttpResponse = httpclient.execute(httpGet)
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  def refreshTangs(): Unit = {
//    val refreshUrl = "http://42.236.61.123:8802/data/flush/tangs"
    val refreshUrl = properties.getProperty("interface.tangs")
    try {
      val httpGet = new HttpGet(refreshUrl)

//      import org.apache.http.client.config.RequestConfig
//      val requestConfig: RequestConfig = RequestConfig.custom.setConnectTimeout(1000).setConnectionRequestTimeout(1000).setSocketTimeout(1000).build
//      httpGet.setConfig(requestConfig)

      val httpResponse: CloseableHttpResponse = httpclient.execute(httpGet)
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  def refreshAlarm(): Unit = {
    val refreshUrl = properties.getProperty("interface.alarm")
    try {
      val httpGet = new HttpGet(refreshUrl)
      val httpResponse: CloseableHttpResponse = httpclient.execute(httpGet)
    } catch {
      case e: Exception =>
        try {
          val httpGet = new HttpGet(refreshUrl)
          val httpResponse: CloseableHttpResponse = httpclient.execute(httpGet)
        } catch {
          case e1: Exception =>

        }
    }
  }

  def main(args: Array[String]): Unit = {
//    refreshTangs()
    refreshBuildDisposal()
  }
}
