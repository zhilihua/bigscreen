package com.gkgd.bigscreen.construction.realtime.out

import java.util.Properties

import com.gkgd.bigscreen.constant.{ESConstant, KafkaConstant}
import com.gkgd.bigscreen.util.{MyEsUtil, MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ModelName
  * @Description
  * @Author zhangjinhang
  * @Date 2020/11/20 18:42
  * @Version V1.0.0
  */
object DdwDataBus2EsStream {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("DdwDataBus2EsStream")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic = properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_BUS)
    val groupId = "dgegwef"
    //获取jt8080数据
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    recordInputStream.foreachRDD{rdd=>
          rdd.foreachPartition{ dataBusBean=>
           val list: List[String] = dataBusBean.toList.map(_.value())
            MyEsUtil.bulkDocWithoutId(list,ESConstant.ES_INDEX_DWD_TRACKS_VEHICLE_POSITION,ESConstant.ES_TYPE_DWD_TRACKS)
          }
        }

    ssc.start()
    ssc.awaitTermination()
  }

}
