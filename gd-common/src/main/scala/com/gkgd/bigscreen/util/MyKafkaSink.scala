package com.gkgd.bigscreen.util

/**
  * @ModelName
  * @Description
  * @Author zhangjinhang
  * @Date 2020/11/20 15:38
  * @Version V1.0.0
  */
import java.util.Properties

import com.gkgd.bigscreen.constant.ConfigConstant
import com.gkgd.bigscreen.util.MyKafkaUtil.properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MyKafkaSink {
  private val properties: Properties = PropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty(ConfigConstant.KAFKA_BOOTSTRAP_SERVERS)

  var kafkaProducer: KafkaProducer[String, String] = null

  def createKafkaProducer: KafkaProducer[String, String] = {
    val properties = new Properties
    properties.put("bootstrap.servers", broker_list)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("enable.idompotence",(true: java.lang.Boolean))
    var producer: KafkaProducer[String, String] = null
    try
      producer = new KafkaProducer[String, String](properties)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    producer
  }

  def send(topic: String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))

  }

  def send(topic: String,key:String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic,key, msg))

  }
}
