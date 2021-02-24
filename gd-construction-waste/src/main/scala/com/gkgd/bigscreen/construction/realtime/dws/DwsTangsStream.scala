package com.gkgd.bigscreen.construction.realtime.dws

import java.lang
import java.text.SimpleDateFormat
import java.util.{Properties, UUID}

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.gkgd.bigscreen.constant.{ESConstant, KafkaConstant}
import com.gkgd.bigscreen.entity.dwd.DataBusBean
import com.gkgd.bigscreen.entity.dws.{SiteBean, TangBean}
import com.gkgd.bigscreen.util._
import net.sf.cglib.beans.BeanCopier
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.gavaghan.geodesy.Ellipsoid
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * @ModelName  趟数
  * @Description
  * @Author zhangjinhang
  * @Date 2020/11/21 16:39
  * @Version V1.0.0
  */
object DwsTangsStream {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("ODS TRACK STREAM5")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic = properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_BUS)
    val groupId = "gddfadfherf"

    //获取数据
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    val dataBusStream: DStream[DataBusBean] = recordInputStream.map { record =>
      val jsonString: String = record.value()
      val dataBusBean: DataBusBean = JSON.parseObject(jsonString, classOf[DataBusBean])
      dataBusBean
    }.filter{ record =>
      record.getAudit_state.equals("1") && record.getManage_state.equals("1")
    }

    val joinSiteStream: DStream[SiteBean] = dataBusStream.mapPartitions { dataBusBean =>
      val siteBeanList: List[SiteBean] = dataBusBean.toList.map { x =>
        val siteBean = new SiteBean
        val copier: BeanCopier = BeanCopier.create(classOf[DataBusBean], classOf[SiteBean], false)
        copier.copy(x, siteBean, null)
        siteBean
      }
      if (siteBeanList.size > 0) {
        //每分区的操作(deptId-vehicelId)
        val deptIdDriverIdList: List[Integer] = siteBeanList.filter(_.dept_id != null).map { data =>
          data.dept_id
        }.toSet.toList
        val deptIds: String = deptIdDriverIdList.mkString(",")
        val sql =s"""
                    |SELECT
                    | build_site_id AS site_id,
                    | build_site_name AS site_name,
                    | address,
                    | lng,
                    | lat,
                    | radius,
                    | province_id,
                    | city_id,
                    | area_id,
                    | dept_id,
                    | 1 AS site_flag
                    | FROM
                    | dim_cwp_d_build_site_info
                    | WHERE is_delete=0 AND dept_id IN($deptIds)
                    | UNION ALL
                    | SELECT
                    | disposal_site_id AS site_id,
                    | disposal_site_name AS site_name,
                    | address,
                    | lng,
                    | lat,
                    | radius,
                    | province_id,
                    | city_id,
                    | area_id,
                    | dept_id,
                    | 2 AS site_flag
                    | FROM
                    | dim_cwp_d_disposal_site_info
                    | WHERE is_delete=0 AND dept_id IN($deptIds)
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

        for (siteBean <- siteBeanList) {
          val jsonObjs: List[JSONObject] = hashMap.getOrElse(siteBean.dept_id, null)
          if (jsonObjs != null) {
            //循环遍历
            var flag = true
            val lng: lang.Double = siteBean.getLng
            val lat: lang.Double = siteBean.getLat

            for (json <- jsonObjs if flag) {
              val siteId = json.getString("site_id")
              val siteName: String = json.getString("site_name")
              val address: String = json.getString("address")
              val siteLng: lang.Double = json.getDouble("lng")
              val siteLat: lang.Double = json.getDouble("lat")
              val radius: Integer = json.getInteger("radius")
              val provinceId: String = json.getString("province_id")
              val cityId = json.getString("city_id")
              val areaId: String = json.getString("area_id")
              val siteFlag: Integer = json.getInteger("site_flag")
//              val time: String = json.getString("time")
              if(lng!=null && lat!=null && siteLng!=null && siteLat!=null){
                val distanceMeter: Double = GeoUtil.getDistanceMeter(lng, lat, siteLng, siteLat, Ellipsoid.Sphere)
                if (distanceMeter <= 500) {
                  siteBean.setSite_id(siteId)
                  siteBean.setSite_name(siteName)
                  siteBean.setSite_address(address)
                  siteBean.setSite_lng(siteLng)
                  siteBean.setSite_lat(siteLat)
                  siteBean.setSite_province_id(provinceId)
                  siteBean.setSite_city_id(cityId)
                  siteBean.setSite_area_id(areaId)
                  siteBean.setSite_flag(siteFlag)
                  //                siteBean.setInterTime(time)
                  flag = false
                }
              }
            }
          }
        }
        siteBeanList.toIterator
      } else {
        siteBeanList.toIterator
      }
    }

    joinSiteStream.foreachRDD{rdd=>
      rdd.foreachPartition{ jsonObjItr=>
        val jsonList: List[SiteBean] = jsonObjItr.toList
        if(jsonList!=null && jsonList.size>0){
          val jedis: Jedis = RedisUtil.getJedisClient
          val filteredList=new ListBuffer[JSONObject]()
          for (siteBean <- jsonList) {
            val siteId: String = siteBean.getSite_id
            val deptId: Integer = siteBean.getDept_id
            val vehicleId: Integer = siteBean.getVehicle_id
            val key = "gkgd:tangs"
            val filed = deptId+":"+vehicleId
            //历史状态
            val siteState: String = jedis.hget(key,filed)

            //1、如果siteId为空，siteState为空，，    路上跑着，不处理
            if(siteId==null && siteState==null){

            }
            //2、如果siteId不为空，siteState为空，，   进入
            if(siteId!=null && siteState==null) {
              siteBean.setWork_state(1)
              siteBean.setInter_time(siteBean.getTime)
              jedis.hset(key,filed,JSON.toJSONString(siteBean,SerializerFeature.WriteMapNullValue))
            }
            //4、如果siteId不为空，siteState不为空，，  工作
            if(siteId!=null && siteState!=null) {
              val siteBeanState: SiteBean = JSON.parseObject(siteState, classOf[SiteBean])
              val siteIdState: String = siteBeanState.getSite_id()
              //判断是否是同一个场地
              val siteId: String = siteBean.getSite_id
              //id相同，工作中

              //id不相同，进入不同场地    趟数
              val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              //进入的场地和时间
              val intoSiteTime: Long = simpleDateFormat.parse(siteBean.getTime).getTime
              //状态后端的场地和时间
              val intoStateSiteTime: Long = simpleDateFormat.parse(siteBeanState.getTime).getTime
              //两个场地的时间差
              val diff: Long = intoSiteTime - intoStateSiteTime
              //这样得到的差值是毫秒级别
              val minute: Long = diff / 1000 / 60
              if(minute<=5){   //时间差小于15分钟,并且进入同一个场地，，工作中
                if(siteId.equals(siteIdState)){
                  siteBeanState.setWork_state(2)
                  siteBeanState.setWork_time(siteBean.getTime)
                  jedis.hset(key,filed,JSON.toJSONString(siteBeanState,SerializerFeature.WriteMapNullValue))
                }
              }
              if(minute>5){  //时间差大于15分钟
                  if(!siteId.equals(siteIdState)){
                    val tangBean = new TangBean
                    val copier: BeanCopier = BeanCopier.create(classOf[SiteBean], classOf[TangBean], false)
                    copier.copy(siteBean, tangBean, null)
                    //设置
                    val vehicleModeId: String = siteBean.getVehicle_model_id()
                    if(vehicleModeId!=null && (vehicleModeId.equals("0") || vehicleModeId.equals("16"))){
                      tangBean.setLoad(19.32D)
                    }
                    if(vehicleModeId!=null && vehicleModeId.equals("17")){
                      tangBean.setLoad(14.90D)
                    }
                    tangBean.setFrom_site_id(siteBeanState.getSite_id)
                    tangBean.setFrom_site_name(siteBeanState.getSite_name)
                    tangBean.setFrom_site_province_id(siteBeanState.getSite_province_id)
                    tangBean.setFrom_site_city_id(siteBeanState.getSite_city_id)
                    tangBean.setFrom_site_area_id(siteBeanState.getSite_area_id)
                    tangBean.setFrom_site_address(siteBeanState.getSite_address)
                    tangBean.setFrom_site_lng(siteBeanState.getSite_lng)
                    tangBean.setFrom_site_lat(siteBeanState.getSite_lat)
                    tangBean.setFrom_site_radius(siteBeanState.getSite_radius)
                    tangBean.setFrom_sit_flag(siteBeanState.getSite_flag)
                    tangBean.setFrom_site_inter_time(siteBeanState.getTime)
                    tangBean.setFrom_site_work_time(siteBeanState.getWork_time)
                    tangBean.setFrom_site_leave_time(siteBeanState.getLeave_time)

                    tangBean.setTo_site_id(siteBean.getSite_id)
                    tangBean.setTo_site_name(siteBean.getSite_name)
                    tangBean.setTo_site_province_id(siteBean.getSite_province_id)
                    tangBean.setTo_site_city_id(siteBean.getSite_city_id)
                    tangBean.setTo_site_area_id(siteBean.getSite_area_id)
                    tangBean.setTo_site_address(siteBean.getSite_address)
                    tangBean.setTo_site_lng(siteBean.getSite_lng)
                    tangBean.setTo_site_lat(siteBean.getSite_lat)
                    tangBean.setTo_site_radius(siteBean.getSite_radius)
                    tangBean.setTo_sit_flag(siteBean.getSite_flag)
                    tangBean.setTo_site_inter_time(siteBean.getTime)
                    tangBean.setTo_site_work_time(siteBean.getWork_time)
                    tangBean.setTo_site_leave_time(siteBean.getLeave_time)

                    if(siteBeanState.getSite_flag==1){
                      //1 空载 0满载
                      tangBean.setVehicle_empty(0)
                    }else{
                      tangBean.setVehicle_empty(1)
                    }
                    //
                    //写入Kafka中
                    val dataBusJsonString: String  = JSON.toJSONString(tangBean,SerializerFeature.WriteMapNullValue)
                    println(dataBusJsonString)
                    MyKafkaSink.send(properties.getProperty(KafkaConstant.TOPIC_DWS_DATA_TANGS), dataBusJsonString)

                    //删除状态
                    jedis.hdel(key,filed)
                  }
              }
            }
            //3、如果siteId为空，siteState不为空，，   离开，不处理
            if(siteId==null && siteState!=null) {
              siteBean.setWork_state(0)
              jedis.hset(key,filed,JSON.toJSONString(siteBean,SerializerFeature.WriteMapNullValue))
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
