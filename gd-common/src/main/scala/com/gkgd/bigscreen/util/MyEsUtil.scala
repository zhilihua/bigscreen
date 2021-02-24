package com.gkgd.bigscreen.util

/**
  * @ModelName
  * @Description
  * @Author zhangjinhang
  * @Date 2020/11/20 16:36
  * @Version V1.0.0
  */
import java.util
import java.util.Properties

import com.gkgd.bigscreen.constant.ConfigConstant
import com.gkgd.bigscreen.entity.dwd.DataBusBean
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {

  private val properties: Properties = PropertiesUtil.load("config.properties")
  val esServerUri = properties.getProperty(ConfigConstant.ES_SERVER_URI)

  var factory:JestClientFactory=null

  def getClient:JestClient ={
    if(factory==null)build()
    factory.getObject

  }

  def  build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(esServerUri)
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(10000).build())

  }

  //单个文档
  def addDoc(source:Any,_index:String,_type:String): Unit ={
    val jest: JestClient = getClient
    //build 设计模式
    // 可转化为json对象    HashMap
    val index = new Index.Builder(source).index(_index).`type`(_type).build()
    val message: String = jest.execute(index).getErrorMessage
    if(message!=null){
      println(message)
    }
    jest.close()
  }
  //批量带id
  def  bulkDoc( sourceList:List[(String,Any)],_index:String,_type:String): Unit ={
    if(sourceList!=null&&sourceList.size>0){
      val jest: JestClient = getClient
      val bulkBuilder = new Bulk.Builder          //构造批次操作
      for ((id,source) <- sourceList ) {
        val index = new Index.Builder(source).index(_index).`type`(_type).id(id).build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      val result: BulkResult = jest.execute(bulk)
      val items: util.List[BulkResult#BulkResultItem] = result.getItems
      println("保存到ES:"+items.size()+"条数")
      jest.close()
    }
  }
  //批量不带id
  def  bulkDocWithoutId( sourceList:List[Any],_index:String,_type:String): Unit ={
    if(sourceList!=null&&sourceList.size>0){
      val jest: JestClient = getClient
      val bulkBuilder = new Bulk.Builder
      for (source <- sourceList ) {
        val index = new Index.Builder(source).index(_index).`type`(_type).build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      val result: BulkResult = jest.execute(bulk)
      val items: util.List[BulkResult#BulkResultItem] = result.getItems
      jest.close()
    }
  }
}
