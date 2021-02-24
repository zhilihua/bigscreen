package com.gkgd.bigscreen.construction.realtime.dws

import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.gkgd.bigscreen.constant.{AlarmConstant, KafkaConstant}
import com.gkgd.bigscreen.entity.dwd.DataBusBean
import com.gkgd.bigscreen.entity.dws.AlarmBean
import com.gkgd.bigscreen.util._
import net.sf.cglib.beans.BeanCopier
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.gavaghan.geodesy.Ellipsoid
import redis.clients.jedis.Jedis


/**
  * @ModelName
  * @Description
  * @Author zhangjinhang
  * @Date 2020/11/21 16:42
  * @Version V1.0.0
  */
object DwsAlarmGpsIncompleteStream {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("ODS TRACK STREAM2")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic = properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_BUS)
    val groupId = "hethehdfg"

    val alarmGpsIncomplete: String = properties.getProperty(AlarmConstant.ALARM_GPS_INCOMPLETE)

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
        val list: List[DataBusBean] = jsonObjItr.toList
        val jedis: Jedis = RedisUtil.getJedisClient
        if(list!=null && list.size>0){
          for ( dataBusBean <- list){
            val speed: Double = dataBusBean.getSpeed.toDouble
            val key = dataBusBean.dept_id+":"+dataBusBean.vehicle_id

            val noGpsState: String = jedis.hget(key,alarmGpsIncomplete)
            if(noGpsState==null && speed==0.0 && dataBusBean.getTime!=null){
              val alarmBean = new AlarmBean
              val copier: BeanCopier = BeanCopier.create(classOf[DataBusBean], classOf[AlarmBean], false)
              copier.copy(dataBusBean, alarmBean, null)
              alarmBean.setIllegal_type_code(alarmGpsIncomplete)
              alarmBean.setAlarm_start_time(dataBusBean.getTime)
              alarmBean.setAlarm_start_lng(dataBusBean.getLng)
              alarmBean.setAlarm_start_lat(dataBusBean.getLat)
              jedis.hset(key,alarmGpsIncomplete,JSON.toJSONString(alarmBean,SerializerFeature.WriteMapNullValue))
            }
            //不做处理
            if(noGpsState!=null && speed==0.0){
            }
            if(noGpsState!=null && speed>0.0){
              val alarmBeanState: AlarmBean = JSON.parseObject(noGpsState, classOf[AlarmBean])

              val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              val intoTime: Long = simpleDateFormat.parse(dataBusBean.getTime).getTime
              //状态后端的时间
              val intoTimeState = simpleDateFormat.parse(alarmBeanState.getAlarm_start_time).getTime()
              val diff = intoTime - intoTimeState; //毫秒级差值
              val minute = diff / 1000 / 60

              if (minute > 3) {
                val distence: Double = GeoUtil.getDistanceMeter(dataBusBean.getLng, dataBusBean.getLat, alarmBeanState.getAlarm_start_lng, alarmBeanState.getAlarm_start_lat, Ellipsoid.Sphere)
                if(distence >2000){
                  alarmBeanState.setAlarm_end_time(dataBusBean.getTime)
                  alarmBeanState.setAlarm_end_lng(dataBusBean.getLng)
                  alarmBeanState.setAlarm_end_lat(dataBusBean.getLat)

                  val key = "alarm:time"
                  val filed = dataBusBean.dept_id+":"+dataBusBean.vehicle_id+":" + alarmGpsIncomplete
                  val alarmTimeState: String = jedis.hget(key,filed)
                  if(alarmTimeState==null){
                    jedis.hset(key,filed,alarmBeanState.getAlarm_start_time)
                    val alarmJsonString: String  = JSON.toJSONString(alarmBeanState,SerializerFeature.WriteMapNullValue)
                    MyKafkaSink.send(properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_ALARM), alarmJsonString)
                  } else {
                    val time: Long = simpleDateFormat.parse(alarmTimeState).getTime
                    val diff = intoTimeState - time; //毫秒级差值
                    val hours = diff / 1000 / 60 / 60
                    if (hours >= 2) {
                      jedis.hset(key,filed,alarmBeanState.getAlarm_start_time)
                      val alarmJsonString: String  = JSON.toJSONString(alarmBeanState,SerializerFeature.WriteMapNullValue)
                      MyKafkaSink.send(properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_ALARM), alarmJsonString)
                    }
                  }
                }
              }
              jedis.hdel(key,alarmGpsIncomplete)
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
