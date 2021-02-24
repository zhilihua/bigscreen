package com.gkgd.bigscreen.construction.realtime.dws

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.gkgd.bigscreen.constant.{AlarmConstant, KafkaConstant}
import com.gkgd.bigscreen.entity.dwd.DataBusBean
import com.gkgd.bigscreen.entity.dws.{AlarmBean, SiteBean, TangBean}
import com.gkgd.bigscreen.util._
import net.sf.cglib.beans.BeanCopier
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @ModelName 围栏告警
  * @Description
  * @Author zhangjinhang
  * @Date 2020/11/21 16:41
  * @Version V1.0.0
  */
object DwsAlarmFenceStream {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("ODS TRACK STREAM1")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic = properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_BUS)
    val groupId = "group_qfeqdg"

    val alarmSpeed: String = properties.getProperty(AlarmConstant.ALARM_FENCE_SPEED)
    val alarmTime: String = properties.getProperty(AlarmConstant.ALARM_FENCE_TIME)
    val alarmInter: String = properties.getProperty(AlarmConstant.ALARM_FENCE_INTER)
    val alarmFile: String = properties.getProperty(AlarmConstant.ALARM_FENCE_FILE)
    val alarmOut: String = properties.getProperty(AlarmConstant.ALARM_FENCE_OUT)

    //获取数据
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    val dataBusStream: DStream[DataBusBean] = recordInputStream.map { record =>
      val jsonString: String = record.value()
      val dataBusBean: DataBusBean = JSON.parseObject(jsonString, classOf[DataBusBean])
      dataBusBean
    }.filter{ record =>
      record.getAudit_state.equals("1") && record.getManage_state.equals("1")
    }

    //是否进入围栏
    val interFenceStream: DStream[AlarmBean] = dataBusStream.mapPartitions { dataBusBean =>
      val alarmBeanList: List[AlarmBean] = dataBusBean.toList.map { x =>
        val alarmBean = new AlarmBean
        val copier: BeanCopier = BeanCopier.create(classOf[DataBusBean], classOf[AlarmBean], false)
        copier.copy(x, alarmBean, null)
        alarmBean
      }

      if (alarmBeanList.size > 0) {
        //每分区的操作(deptId-vehicelId)
        val deptIdDriverIdList: List[Integer] = alarmBeanList.filter(_.dept_id != null).map { data =>
          data.dept_id
        }.toSet.toList
        val deptIds: String = deptIdDriverIdList.mkString(",")

        val sql =s"""
                    | SELECT t1.*,t2.name,t2.coords,t2.area_id
                    | FROM (SELECT condition_id,speed,null as start_date,null as end_date,1 as fence_flag,dept_id from dim_cwp_boundary_condition_over_speed where dept_id in($deptIds) and state=0
                    | UNION ALL
                    | select condition_id,NULL as speed,null as start_date,null as end_date,2 as fenc_flag,dept_id from dim_cwp_boundary_condition_penalty_fence where dept_id in($deptIds) and state=0
                    | UNION ALL
                    | select condition_id,NULL as speed,start_date,end_date,3 as fenc_flag,dept_id from dim_cwp_boundary_condition_work_time where dept_id in($deptIds) and state=0
                    | UNION ALL
                    |  select condition_id,NULL as speed,null as start_date,null as end_date,4 as fenc_flag,dept_id from dim_cwp_boundary_condition_penalty_out_fence where dept_id in($deptIds) and state=0
                    | UNION ALL
                    |  select condition_id,NULL as speed,start_date,end_date,5 as fenc_flag,dept_id from dim_cwp_boundary_condition_document_control where dept_id in($deptIds) and state=0
                    | ) t1
                    | join dim_cwp_boundary_condition t2 on t1.condition_id=t2.id
            """.stripMargin
        val jsonObjList: List[JSONObject] = MysqlUtil.queryList(sql)

        val hashMap = new scala.collection.mutable.HashMap[Integer, List[JSONObject]]
        for (json <- jsonObjList) {
          val deptId: Integer = json.getInteger("dept_id")
          if (hashMap.contains(deptId)) {
            val value: List[JSONObject] = hashMap.get(deptId).get
            val values = value :+ json
            hashMap += (deptId -> values)
          } else {
            hashMap += (deptId -> List(json))
          }
        }

        val lst1 = new ListBuffer[AlarmBean]

        for (alarmBean <- alarmBeanList) {
          val jsonObjs: List[JSONObject] = hashMap.getOrElse(alarmBean.dept_id, null)
          if (jsonObjs != null) {
            val lng: lang.Double = alarmBean.getLng
            val lat: lang.Double = alarmBean.getLat
            var flag = 0
            for (json <- jsonObjs) {
              val fenceCoords: String = json.getString("coords")
              var inArea = false
              if(lng!=null && lat!=null && fenceCoords!=null) {
                inArea = AreaUtil.inArea2(lng, lat, fenceCoords)
              }
              if (inArea) {
                val fenceFlag: Integer = json.getInteger("fence_flag")
                //判断是否超速
                if(fenceFlag==1){
                  val fenceSpeed: Double = json.getDouble("speed")
                  val speed: Double = alarmBean.getSpeed.toDouble
                  if(speed>fenceSpeed){
                    flag = 1
                    alarmBean.setFence_id(json.getString("condition_id"))
                    alarmBean.setFence_name(json.getString("name"))
                    alarmBean.setFence_area_id(json.getString("area_id"))
                    alarmBean.setFence_flag(json.getInteger("fence_flag"))
                    alarmBean.setIllegal_type_code(alarmSpeed)
                    alarmBean.setAlarm_start_time(alarmBean.getTime())
                    alarmBean.setAlarm_start_lng(lng)
                    alarmBean.setAlarm_start_lat(lat)
                    lst1 += alarmBean
                  }
                }
                //判断是否进入禁区
                if(fenceFlag==2){
                  val speed: Double = alarmBean.getSpeed.toDouble
                  if(speed>0){
                    flag = 1
                    alarmBean.setFence_id(json.getString("condition_id"))
                    alarmBean.setFence_name(json.getString("name"))
                    alarmBean.setFence_area_id(json.getString("area_id"))
                    alarmBean.setFence_flag(json.getInteger("fence_flag"))
                    alarmBean.setIllegal_type_code(alarmInter)
                    alarmBean.setAlarm_start_time(alarmBean.getTime())
                    alarmBean.setAlarm_start_lng(lng)
                    alarmBean.setAlarm_start_lat(lat)
                    lst1 += alarmBean
                  }
                }
                //判断是否违规时间
                if(fenceFlag==3){
                  val startDate: String = json.getString("start_date")
                  val endDate: String = json.getString("end_date")
                  val time: String = alarmBean.getTime
                  val inTimeRange = DateUtil.isInTimeRange(time, startDate, endDate)
                  if (!inTimeRange) {
                    flag = 1
                    alarmBean.setFence_id(json.getString("condition_id"))
                    alarmBean.setFence_name(json.getString("name"))
                    alarmBean.setFence_area_id(json.getString("area_id"))
                    alarmBean.setFence_flag(json.getInteger("fence_flag"))
                    alarmBean.setIllegal_type_code(alarmTime)
                    alarmBean.setAlarm_start_time(alarmBean.getTime())
                    alarmBean.setAlarm_start_lng(lng)
                    alarmBean.setAlarm_start_lat(lat)
                    lst1 += alarmBean
                  }
                }
                //闯出禁区
                if(fenceFlag==4){
                  val speed: Double = alarmBean.getSpeed.toDouble
                  if(speed>0){
                    flag = 1
                    alarmBean.setFence_id(json.getString("condition_id"))
                    alarmBean.setFence_name(json.getString("name"))
                    alarmBean.setFence_area_id(json.getString("area_id"))
                    alarmBean.setFence_flag(json.getInteger("fence_flag"))
                    alarmBean.setIllegal_type_code(alarmOut)
                    alarmBean.setAlarm_start_time(alarmBean.getTime())
                    alarmBean.setAlarm_start_lng(lng)
                    alarmBean.setAlarm_start_lat(lat)
                    lst1 += alarmBean
                  }
                }
                //文件时间
                if(fenceFlag==5){
                  val startDate: String = json.getString("start_date")
                  val endDate: String = json.getString("end_date")
                  val time: String = alarmBean.getTime

                  val inTimeRange = DateUtil.isEffectiveDate(time,startDate,endDate)
                  if (inTimeRange) {
                    flag = 1
                    alarmBean.setFence_id(json.getString("condition_id"))
                    alarmBean.setFence_name(json.getString("name"))
                    alarmBean.setFence_area_id(json.getString("area_id"))
                    alarmBean.setFence_flag(json.getInteger("fence_flag"))
                    alarmBean.setIllegal_type_code(alarmFile)
                    alarmBean.setAlarm_start_time(alarmBean.getTime())
                    alarmBean.setAlarm_start_lng(lng)
                    alarmBean.setAlarm_start_lat(lat)
                    lst1 += alarmBean
                  }
                }

              }
            }
            if (flag == 0) {
              lst1 += alarmBean
            }
          } else {
            lst1 += alarmBean
          }
        }
        lst1.toIterator
      } else {
        alarmBeanList.toIterator
      }
    }

    //TODO 判断状态
    interFenceStream.foreachRDD {rdd =>
      rdd.foreachPartition { jsonObjItr =>
        val jsonList: List[AlarmBean] = jsonObjItr.toList
        if(jsonList!=null && jsonList.size>0){
          val jedis: Jedis = RedisUtil.getJedisClient
          val filteredList=new ListBuffer[JSONObject]()
          for (alarmBean <- jsonList) {
            val deptId: Integer = alarmBean.getDept_id
            val vehicleId: Integer = alarmBean.getVehicle_id
            val illegalTypeCode: String = alarmBean.getIllegal_type_code
            //filed: deptId:vehicleId
            val key = deptId+":"+vehicleId

            import scala.collection.JavaConversions.mapAsScalaMap
            val fenceAlarmMap: mutable.Map[String, String] = jedis.hgetAll(key).filter{case(k,v)=>List("0110","0116","0115","0114","0119").contains(k)}

            //1、既没有违规，也没有历史违规，，不做处理
            if(illegalTypeCode==null && (fenceAlarmMap==null || fenceAlarmMap.size==0)){
            }
            //2、没有违规，但有历史违规
            if(illegalTypeCode==null && (fenceAlarmMap!=null && fenceAlarmMap.size>0)){
              for((k,v) <- fenceAlarmMap){
                val alarmBeanState: AlarmBean = JSON.parseObject(v, classOf[AlarmBean])
                val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                val intoTime: Long = simpleDateFormat.parse(alarmBean.getTime).getTime
                //状态后端的时间
                val intoTimeState = simpleDateFormat.parse(alarmBeanState.getAlarm_start_time).getTime()
                val diff = intoTime - intoTimeState; //毫秒级差值
                val minute = diff / 1000 / 60
                if (minute > 3) {
                  alarmBeanState.setAlarm_end_time(alarmBean.getTime)
                  alarmBeanState.setAlarm_end_lng(alarmBean.getLng)
                  alarmBeanState.setAlarm_end_lat(alarmBean.getLat)
                  //TODO Kafka
                  //写入Kafka中
                  //判断超过两个小时或一天
                  val key = "alarm:time"
                  val filed = deptId+":"+vehicleId+":" + alarmBeanState.getIllegal_type_code
                  val alarmTimeState: String = jedis.hget(key,filed)
                  if(alarmTimeState==null){
                    jedis.hset(key,filed,alarmBeanState.getAlarm_start_time)
                    val alarmJsonString: String  = JSON.toJSONString(alarmBeanState,SerializerFeature.WriteMapNullValue)
                    MyKafkaSink.send(properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_ALARM), alarmJsonString)
                  }else{
                    val time: Long = simpleDateFormat.parse(alarmTimeState).getTime
                    val diff = intoTimeState - time; //毫秒级差值
                    val hours = diff / 1000 / 60 / 60
                    if(List("0110","0116","0115","0114").contains(alarmBeanState.getIllegal_type_code)){
                      if (hours >= 2) {
                        jedis.hset(key,filed,alarmBeanState.getAlarm_start_time)
                        val alarmJsonString: String  = JSON.toJSONString(alarmBeanState,SerializerFeature.WriteMapNullValue)
                        MyKafkaSink.send(properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_ALARM), alarmJsonString)
                      }
                    }

                    if(List("0119").contains(alarmBeanState.getIllegal_type_code)){
                      val alarmHistoryDay: String = alarmTimeState.split(" ")(0)
                      val alarmDay = alarmBeanState.getAlarm_start_time.split(" ")(0)
                      if (!alarmHistoryDay.equals(alarmDay)) {
                        jedis.hset(key,filed,alarmBeanState.getAlarm_start_time)
                        val alarmJsonString: String  = JSON.toJSONString(alarmBeanState,SerializerFeature.WriteMapNullValue)
                        MyKafkaSink.send(properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_ALARM), alarmJsonString)
                      }
                    }
                    
                  }
                  
                }
                //删除历史数据
                jedis.hdel(key,k)
              }
            }
            //3、有违规，没有历史违规，，添加历史记录
            if(illegalTypeCode!=null) {
              if(fenceAlarmMap==null || fenceAlarmMap.size==0){
                jedis.hset(key,illegalTypeCode,JSON.toJSONString(alarmBean,SerializerFeature.WriteMapNullValue))
              } else {
                if(!fenceAlarmMap.contains(illegalTypeCode)){
                  jedis.hset(key,illegalTypeCode,JSON.toJSONString(alarmBean,SerializerFeature.WriteMapNullValue))
                }
              }
            }
          }
          jedis.close()
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
