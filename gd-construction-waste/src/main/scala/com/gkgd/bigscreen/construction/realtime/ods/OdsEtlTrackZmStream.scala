package com.gkgd.bigscreen.construction.realtime.ods

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.gkgd.bigscreen.constant.KafkaConstant
import com.gkgd.bigscreen.entity.dwd.DataBusBean
import com.gkgd.bigscreen.util._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * @ModelName
  * @Description
  * @Author zhangjinhang
  * @Date 2020/11/19 17:29
  * @Version V1.0.0
  */
object OdsEtlTrackZmStream {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("ODS TRACK STREAM")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic = properties.getProperty(KafkaConstant.TOPIC_ODS_TRACKS_RAW)
    val groupId = "gewddxczx"

    //获取jt8080数据
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    //将jt8080解析转换成数据总线
    val transportInputStream: DStream[DataBusBean] = recordInputStream.map { record =>
      val jsonString: String = record.value()
      val dataBusBean: DataBusBean = JSON.parseObject(jsonString, classOf[DataBusBean])
      dataBusBean
    }

    //过滤违规数据
    val filterIllegalStream: DStream[DataBusBean] = transportInputStream.filter { dataBusBean =>
      val lng: Double = dataBusBean.getLng
      val lat: Double = dataBusBean.getLat
      val speed: Double = dataBusBean.getSpeed.toDouble
      val time: String = dataBusBean.getTime
      val gpsDate: String = time.split(" ")(0)
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val nowDate: String = sdf.format(new Date)
      //TODO 参数配置文件获取
      if ((lat > 20 && lat < 40) && (lng > 100 && lng < 120) && (speed >= 0 && speed < 100) && nowDate == gpsDate) {
        val gps: Array[Double] = GeoUtil.wgs2bd(lat, lng)
        dataBusBean.setLat(gps(0))
        dataBusBean.setLng(gps(1))
        true
      }
      else {
        false
      }
    }

    //关联车辆表
    val joinVehicleStream: DStream[DataBusBean] = filterIllegalStream.mapPartitions { dataBusBean =>
      val dataBusBeanList: List[DataBusBean] = dataBusBean.toList
      if (dataBusBeanList.size > 0) {
        //每分区的操作
        val carCardNumberList: List[String] = dataBusBeanList.map(_.car_card_number.trim).toSet.toList
        val carCardNumbers: String = carCardNumberList.mkString("','")
        val sql =
          s"""
             |select
             | terminal_id,
             | vehicle_id,
             | car_card_number,
             | vehicle_model_id,
             | vehicle_type_id,
             | vehicle_type_state,
             | vehicle_state,
             | if_new_energy,
             | approved_tonnage,
             | driver_id,
             | enterprise_id,
             | audit_state,
             | manage_state,
             | dept_id
             | from
             | dim_cwp_d_vehicle_info
             | where
             | car_card_number in ('$carCardNumbers')
            """.stripMargin
        val jsonObjList: List[JSONObject] = MysqlUtil.queryList(sql)
        val vehicleMap: Map[String, JSONObject] = jsonObjList.map(jsonObj => (jsonObj.getString("car_card_number"), jsonObj)).toMap

        val lst1 = new ListBuffer[DataBusBean]
        if(vehicleMap!=null && vehicleMap.size>0){
          for (dataBusBean <- dataBusBeanList) {
            val vehicleObj: JSONObject = vehicleMap.getOrElse(dataBusBean.car_card_number, null)
            if(vehicleObj!=null){
              dataBusBean.devid = vehicleObj.getString("terminal_id")
              dataBusBean.vehicle_id = vehicleObj.getInteger("vehicle_id")
//              dataBusBean.carCardNumber = vehicleObj.getString("car_card_number")
              dataBusBean.vehicle_model_id = vehicleObj.getString("vehicle_model_id")
              dataBusBean.vehicle_type_id = vehicleObj.getString("vehicle_type_id")
              dataBusBean.vehicle_type_state = vehicleObj.getString("vehicle_type_state")
              dataBusBean.vehicle_state = vehicleObj.getString("vehicle_state")
              dataBusBean.if_new_energy = vehicleObj.getInteger("if_new_energy")
              dataBusBean.approved_tonnage = vehicleObj.getFloat("approved_tonnage")
              dataBusBean.driver_id = vehicleObj.getInteger("driver_id")
              dataBusBean.enterprise_id = vehicleObj.getString("enterprise_id")
              dataBusBean.dept_id = vehicleObj.getInteger("dept_id")
              dataBusBean.audit_state = vehicleObj.getString("audit_state")
              dataBusBean.manage_state = vehicleObj.getString("manage_state")
              lst1 += dataBusBean
            }
          }
          lst1.toIterator
        }else{
          lst1.toIterator
        }
      }
      else{
        dataBusBeanList.toIterator
      }
    }

    //过滤掉服务费到期车辆
    val filterServiceFeeStream: DStream[DataBusBean] = transportInputStream.mapPartitions { dataBusBean =>
      val dataBusBeanList: List[DataBusBean] = dataBusBean.toList

      if (dataBusBeanList.size > 0) {
        //每分区的操作(deptId-vehicelId)
        val deptIdVhicleIdList: List[String] = dataBusBeanList.map { data =>
          data.dept_id + "-" + data.vehicle_id
        }.toSet.toList
        val deptIdVhicleIds: String = deptIdVhicleIdList.mkString("','")
        val sql =
          s"""
             |select
             | vehicle_id,
             | dept_id,
             | state
             | from
             | ods_cwp_vehicle_service_fee
             | where
             | CONCAT_WS("-",dept_id,vehicle_id) in ('$deptIdVhicleIds')
             | and
             | state=0
            """.stripMargin
        val jsonObjList: List[JSONObject] = MysqlUtil.queryList(sql)
        val vehicleMap: Map[String, JSONObject] = jsonObjList.map(jsonObj => (jsonObj.getString("dept_id") + "-" + jsonObj.getString("vehicle_id"), jsonObj)).toMap

        val lst1 = new ListBuffer[DataBusBean]
        if (vehicleMap != null && vehicleMap.size > 0) {
          for (dataBusBean <- dataBusBeanList) {
            val vehicleObj: JSONObject = vehicleMap.getOrElse(dataBusBean.dept_id + "-" + dataBusBean.vehicle_id, null)
            if (vehicleObj != null) {
              dataBusBean.service_state = 1
              lst1 += dataBusBean
            }
          }
          lst1.toIterator
        } else {
          lst1.toIterator
        }
      }
      else {
        dataBusBeanList.toIterator
      }
    }

    //关联司机表
    val joinDriverStream: DStream[DataBusBean] = filterServiceFeeStream.mapPartitions { dataBusBean =>
      val dataBusBeanList: List[DataBusBean] = dataBusBean.toList
      if (dataBusBeanList.size > 0) {
        //每分区的操作(deptId-vehicelId)
        val deptIdDriverIdList: List[String] = dataBusBeanList.filter(_.driver_id!=null).map { data =>
          data.dept_id + "-" + data.driver_id
        }.toSet.toList
        val deptIdDriverIds: String = deptIdDriverIdList.mkString("','")
        val sql =
          s"""
             |select
             | driver_id,
             | driver_card_number,
             | driver_name,
             | dept_id
             | from
             | dim_cwp_d_driver_info
             | where
             | CONCAT_WS("-",dept_id,driver_id) in ('$deptIdDriverIds')
            """.stripMargin
        val jsonObjList: List[JSONObject] = MysqlUtil.queryList(sql)
        val vehicleMap: Map[String, JSONObject] = jsonObjList.map(jsonObj => (jsonObj.getString("dept_id") + "-" + jsonObj.getString("driver_id"), jsonObj)).toMap
        for (dataBusBean <- dataBusBeanList) {
          val vehicleObj: JSONObject = vehicleMap.getOrElse(dataBusBean.dept_id + "-" + dataBusBean.driver_id, null)
          if (vehicleObj != null) {
            dataBusBean.driver_card_number = vehicleObj.getString("driver_card_number")
            dataBusBean.driver_name = vehicleObj.getString("driver_name")
          }
        }
      }
      dataBusBeanList.toIterator
    }
    //关联公司表
    val joinEnterpriseStream: DStream[DataBusBean] = joinDriverStream.mapPartitions { dataBusBean =>
      val dataBusBeanList: List[DataBusBean] = dataBusBean.toList
      if (dataBusBeanList.size > 0) {
        //每分区的操作(deptId-vehicelId)
        val deptIdDriverIdList: List[String] = dataBusBeanList.filter(_.enterprise_id != null).map { data =>
          data.dept_id + "-" + data.enterprise_id
        }.toSet.toList
        val deptIdEnterpriseIds: String = deptIdDriverIdList.mkString("','")
        val sql =
          s"""
             |select
             | enterprise_id,
             | enterprise_name,
             | department_id,
             | address,
             | lng,
             | lat,
             | enterprise_type_id,
             | province_id,
             | city_id,
             | area_id,
             | dept_id
             | from
             | dim_cwp_d_enterprise_info
             | where
             | CONCAT_WS("-",dept_id,enterprise_id) in ('$deptIdEnterpriseIds')
            """.stripMargin
        val jsonObjList: List[JSONObject] = MysqlUtil.queryList(sql)
        val vehicleMap: Map[String, JSONObject] = jsonObjList.map(jsonObj => (jsonObj.getString("dept_id") + "-" + jsonObj.getString("enterprise_id"), jsonObj)).toMap

        val lst1 = new ListBuffer[DataBusBean]
        for (dataBusBean <- dataBusBeanList) {
          val vehicleObj: JSONObject = vehicleMap.getOrElse(dataBusBean.dept_id + "-" + dataBusBean.enterprise_id, null)
          if (vehicleObj != null) {
            dataBusBean.enterprise_name = vehicleObj.getString("enterprise_name")
            dataBusBean.department_id = vehicleObj.getInteger("department_id")
            dataBusBean.address = vehicleObj.getString("address")
            dataBusBean.enterprise_lng = vehicleObj.getDouble("lng")
            dataBusBean.enterprise_lat = vehicleObj.getDouble("lat")
            dataBusBean.enterprise_type_id = vehicleObj.getString("enterprise_type_id")
            dataBusBean.province_id = vehicleObj.getString("province_id")
            dataBusBean.city_id = vehicleObj.getString("city_id")
            dataBusBean.area_id = vehicleObj.getString("area_id")
            lst1 += dataBusBean
          }
        }
        lst1.toIterator
      } else {
        dataBusBeanList.toIterator
      }
    }
    //5,关联双向登记卡表
    val joinRegisterCardStream: DStream[DataBusBean] = joinEnterpriseStream.mapPartitions { dataBusBean =>
      val dataBusBeanList: List[DataBusBean] = dataBusBean.toList
      if (dataBusBeanList.size > 0) {
        //每分区的操作(deptId-vehicelId)
        val deptIdVehicleIdList: List[String] = dataBusBeanList.filter(_.vehicle_id != null).map { data =>
          data.dept_id + "-" + data.vehicle_id
        }.toSet.toList
        val deptIdVehicleIds: String = deptIdVehicleIdList.mkString("','")

        //          val sql =
        //            s"""
        //               |select
        //               | vehicle_id,
        //               | coords,
        //               | state,
        //               | dept_id
        //               | from
        //               | ods_cwp_vehicle_register_card
        //               | where
        //               | CONCAT_WS("-",dept_id,vehicle_id) in ('$deptIdVehicleIds')
        //               | and
        //               | state=0
        //            """.stripMargin
        val sql =
        s"""
           |select
           | vehicle_id,
           | state,
           | dept_id
           | from
           | ods_cwp_vehicle_register_card
           | where
           | CONCAT_WS("-",dept_id,vehicle_id) in ('$deptIdVehicleIds')
           | and
           | state=0
            """.stripMargin
        val jsonObjList: List[JSONObject] = MysqlUtil.queryList(sql)
        //一辆车可以有多张双向登记卡，一张双向登记卡有多条路线
        val hashMap = new scala.collection.mutable.HashMap[String, String]
        for (josn <- jsonObjList) {
          val deptId: Integer = josn.getInteger("dept_id")
          val vehicleId: Integer = josn.getInteger("vehicle_id")

          //            val coords: String = josn.getString("coords")
          val coords: String = ""

          val key = deptId + "-" + vehicleId
          if (hashMap.contains(key)) {
            var lines: String = hashMap.get(key).get
            lines = lines + "|" + coords
            hashMap += (key -> lines)
          } else {
            hashMap += (key -> coords)
          }
        }
        for (dataBusBean <- dataBusBeanList) {
          val lines: String = hashMap.getOrElse(dataBusBean.dept_id + "-" + dataBusBean.vehicle_id, null)
          if (lines != null) {
            dataBusBean.register_card_state = 1
            //              dataBusBean.coords = lines
          }
        }
      }
      dataBusBeanList.toIterator
    }

    // 6,关联运输证表
    val joinTransportCardStream: DStream[DataBusBean] = joinRegisterCardStream.mapPartitions { dataBusBean =>
      val dataBusBeanList: List[DataBusBean] = dataBusBean.toList
      if (dataBusBeanList.size > 0) {
        //每分区的操作(deptId-vehicelId)
        val deptIdVehicleIdList: List[String] = dataBusBeanList.filter(_.vehicle_id != null).map { data =>
          data.dept_id + "-" + data.vehicle_id
        }.toSet.toList
        val deptIdVehicleIds: String = deptIdVehicleIdList.mkString("','")
        val sql =
          s"""
             |select
             | vehicle_id,
             | state,
             | dept_id
             | from
             | ods_cwp_vehicle_transport_card
             | where
             | CONCAT_WS("-",dept_id,vehicle_id) in ('$deptIdVehicleIds')
             | and
             | state=1
            """.stripMargin
        val jsonObjList: List[JSONObject] = MysqlUtil.queryList(sql)
        //一车一证
        val vehicleMap: Map[String, JSONObject] = jsonObjList.map(jsonObj => (jsonObj.getString("dept_id") + "-" + jsonObj.getString("vehicle_id"), jsonObj)).toMap
        for (dataBusBean <- dataBusBeanList) {
          val vehicleObj: JSONObject = vehicleMap.getOrElse(dataBusBean.dept_id + "-" + dataBusBean.vehicle_id, null)
          if (vehicleObj != null) {
            dataBusBean.transport_card_state = 1
          }
        }
      }
      dataBusBeanList.toIterator
    }

    // 7,关联区域表
    val dataBusStream: DStream[DataBusBean] = joinTransportCardStream.mapPartitions { dataBusBean =>
      val dataBusBeanList: List[DataBusBean] = dataBusBean.toList
      if (dataBusBeanList.size > 0) {
        val deptIdList: List[Integer] = dataBusBeanList.filter(_.dept_id != null).map { data =>
          data.dept_id
        }.toSet.toList
        val deptIds: String = deptIdList.mkString(",")
        val sql =
          s"""
             |select
             | id,
             | city_name,
             | coords,
             | dept_id
             | from
             | dim_cwp_area_coords
             | where
             | dept_id in ($deptIds)
             | and
             | state=1
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
        for (dataBusBean <- dataBusBeanList) {
          val jsonObjs: List[JSONObject] = hashMap.getOrElse(dataBusBean.dept_id, null)
          if (jsonObjs != null) {
            //循环遍历
            var flag = true
            val lng = dataBusBean.getLng
            val lat = dataBusBean.getLat
            for (json <- jsonObjList if flag) {
              val areaId: String = json.getString("id")
              val cityName: String = json.getString("city_name")
              val coords: String = json.getString("coords")
              val bool = AreaUtil.inArea(lng, lat, coords)
              if (bool) {
                dataBusBean.region_id = areaId
                dataBusBean.region_name = cityName
                flag = false
              }
            }
          }
        }
      }
      dataBusBeanList.toIterator
    }

    dataBusStream.foreachRDD{rdd=>
      rdd.foreachPartition{ orderInfoItr=>
        val dataBusBeanList: List[DataBusBean] = orderInfoItr.toList
        for (dataBusBean <- dataBusBeanList ) {
          val dataBusJsonString: String  = JSON.toJSONString(dataBusBean,SerializerFeature.WriteMapNullValue)
          MyKafkaSink.send(properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_BUS), dataBusJsonString)
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
