package com.gkgd.bigscreen.construction.realtime.dws
import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.gkgd.bigscreen.constant.{AlarmConstant, KafkaConstant}
import com.gkgd.bigscreen.entity.dwd.DataBusBean
import com.gkgd.bigscreen.entity.dws.AlarmBean
import com.gkgd.bigscreen.util.{MyKafkaSink, MyKafkaUtil, PropertiesUtil, RedisUtil}
import net.sf.cglib.beans.BeanCopier
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis
/**
  * @ModelName
  * @Description
  * @Author zhangjinhang
  * @Date 2020/11/21 16:43
  * @Version V1.0.0
  */
object DwsAlarmNoRegisterStream {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("ODS TRACK STREAM3")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic = properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_BUS)
    val groupId = "ehrgqgdfs"

    val alarmNoCard: String = properties.getProperty(AlarmConstant.ALARM_REGISTER_CARD)

    //获取数据
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    val dataBusStream: DStream[DataBusBean] = recordInputStream.map { record =>
      val jsonString: String = record.value()
      val dataBusBean: DataBusBean = JSON.parseObject(jsonString, classOf[DataBusBean])
      dataBusBean
    }.filter{ record =>
      record.getAudit_state.equals("1") && record.getManage_state.equals("1")
    }

    dataBusStream.foreachRDD {rdd =>
      rdd.foreachPartition { jsonObjItr =>
        val jedis: Jedis = RedisUtil.getJedisClient
        val list: List[DataBusBean] = jsonObjItr.toList
        if(list!=null && list.size>0){
          for ( dataBusBean <- list){
            val registerCardState: Integer = dataBusBean.getRegister_card_state
            if(registerCardState==0){
              val key = "alarm:time"
              val filed = dataBusBean.dept_id+":"+dataBusBean.vehicle_id+":" + alarmNoCard
              val alarmTimeState: String = jedis.hget(key,filed)
              if(alarmTimeState==null){
                val alarmBean = new AlarmBean
                val copier: BeanCopier = BeanCopier.create(classOf[DataBusBean], classOf[AlarmBean], false)
                copier.copy(dataBusBean, alarmBean, null)
                alarmBean.setIllegal_type_code(alarmNoCard)
                alarmBean.setAlarm_start_time(alarmBean.getTime)
                alarmBean.setAlarm_start_lng(alarmBean.getLng)
                alarmBean.setAlarm_start_lat(alarmBean.getLat)

                jedis.hset(key,filed,dataBusBean.getTime)
                MyKafkaSink.send(properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_ALARM), JSON.toJSONString(alarmBean, SerializerFeature.WriteMapNullValue))
              } else {
                val alarmHistoryDay: String = alarmTimeState.split(" ")(0)
                val alarmDay = dataBusBean.getTime.split(" ")(0)
                if (!alarmHistoryDay.equals(alarmDay)) {
                  val alarmBean = new AlarmBean
                  val copier: BeanCopier = BeanCopier.create(classOf[DataBusBean], classOf[AlarmBean], false)
                  copier.copy(dataBusBean, alarmBean, null)
                  alarmBean.setIllegal_type_code(alarmNoCard)
                  alarmBean.setAlarm_start_time(alarmBean.getTime)
                  alarmBean.setAlarm_start_lng(alarmBean.getLng)
                  alarmBean.setAlarm_start_lat(alarmBean.getLat)

                  jedis.hset(key,filed,dataBusBean.getTime)
                  MyKafkaSink.send(properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_ALARM), JSON.toJSONString(alarmBean, SerializerFeature.WriteMapNullValue))
                }
              }
            }
          }
        }
        jedis.close()
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
