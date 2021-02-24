package com.gkgd.bigscreen.construction.realtime.out

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.alibaba.fastjson.JSON
import com.gkgd.bigscreen.constant.{ConfigConstant, KafkaConstant}
import com.gkgd.bigscreen.entity.dws.TangBean
import com.gkgd.bigscreen.util.{MyKafkaUtil, PropertiesUtil, RefreshUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ModelName
  * @Description
  * @Author zhangjinhang
  * @Date 2020/11/21 16:43
  * @Version V1.0.0
  */
object DwsTangs2HiveStream {
  case class Tang(device_id:String,
                  time:String,
                  gps_lng:String,
                  gps_lat:String,
                  speed:String,
                  pos:String,
                  gps_flag:String,
                  wire_flag:String,
                  power_flag:String,
                  acc_flag:String,
                  auxiliary_flag:String,
                  temperature:String,
                  door:String,
                  all_distance:String,
                  all_dist_fuel:String,
                  gps_distance:String,
                  gps_all_distance:String,
                  report_time:String,
                  zxcqi:String,
                  alarm_flag:String,
                  main_oil_per:String,
                  residual_oil:String,
                  vice_oil_per:String,
                  vice_residual_oil:String,
                  oil_difference:String,
                  sharp_turn:String,
                  brake:String,
                  e_site_time:String,
                  e_site_id:String,
                  e_site_name:String,
                  e_site_address:String,
                  e_site_province_id:String,
                  e_site_city_id:String,
                  e_site_area_id:String,
                  e_site_lat:Double,
                  e_site_lng:Double,
                  e_site_flag:Int,
                  status:Int,
                  dev_id:String,
                  dev_status:Int,
                  s_site_id:String,
                  s_site_name:String,
                  s_site_flag:Int,
                  s_site_lng:Double,
                  s_site_lat:Double,
                  s_site_area_id:String,
                  s_site_province_id:String,
                  s_site_city_id:String,
                  s_site_address:String,
                  s_site_time:String,
                  updatetime:String,
                  dept_id:Int,
                  vehicle_id:Int,
                  transport_enterprise_id:String,
                  car_card_number:String,
                  load:Double,
                  enterprise_name:String,
                  province_id:String,
                  city_id:String,
                  area_id:String,
                  vehicle_empty:Int,
                  month_id:String,
                  day_id:String,
                  hour_id:String)
  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")
    val hiveMetastore = properties.getProperty(ConfigConstant.HIVE_METASTORE_URIS)
    val hiveDatabase = properties.getProperty(ConfigConstant.HIVE_DATABASE)
    val sparkSession = SparkSession.builder()
      .appName("HiveWrfdfdite")
//      .master("local[4]")
//      .config("spark.debug.maxToStringFields", "100")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("hive.metastore.uris", hiveMetastore)
//      .config("spark.sql.warehouse.dir", "http://192.168.10.20:8080/#/main/dashboard/metrics")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    val ssc = new StreamingContext(sparkContext,Seconds(5))

    val topic = properties.getProperty(KafkaConstant.TOPIC_DWS_DATA_TANGS)
    val groupId = "gdjgefhdeen"

    //获取数据
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    val tangsBeanStream: DStream[TangBean] = recordInputStream.map { record =>
      val jsonString: String = record.value()
      val tangsBean: TangBean = JSON.parseObject(jsonString, classOf[TangBean])
      tangsBean
    }.filter{x =>
      if(x.from_site_lng!=null && x.from_site_lat!=null && x.to_site_lng!=null && x.to_site_lat!=null){
        true
      }else {
        false
      }
    }

    tangsBeanStream.foreachRDD { rdd =>
      val now: Date = new Date()
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = dateFormat.format(now)
      val calendar = Calendar.getInstance()
      val dayz = calendar.get(Calendar.DATE).toString
      val hourz = calendar.get(Calendar.HOUR_OF_DAY).toString
      val split_str = date.split(" ")
      val split_1 = split_str(0).split("-")
      val yearz_month = split_1(0) + "-" + split_1(1)

      import sparkSession.implicits._
      val frame: DataFrame = rdd.map(bean => Tang(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        bean.to_site_inter_time,
        bean.to_site_id,
        bean.to_site_name,
        bean.to_site_address,
        bean.to_site_province_id,
        bean.to_site_city_id,
        bean.to_site_area_id,
        bean.to_site_lat,
        bean.to_site_lng,
        if(bean.to_sit_flag==null) -1 else bean.to_sit_flag,
        0,
        null,
        0,
        bean.from_site_id,
        bean.from_site_name,
        if(bean.from_sit_flag==null) -1 else bean.from_sit_flag,
        bean.from_site_lng,
        bean.from_site_lat,
        bean.from_site_area_id,
        bean.from_site_province_id,
        bean.from_site_city_id,
        bean.from_site_address,
        if(bean.from_site_inter_time==null) "" else bean.from_site_inter_time,
        date,
        bean.dept_id,
        bean.vehicle_id,
        bean.enterprise_id,
        bean.car_card_number,
        bean.load,
        bean.enterprise_name,
        bean.province_id,
        bean.city_id,
        bean.area_id,
        bean.vehicle_empty,
        yearz_month,
        dayz,
        hourz
      )).toDF()

      frame.write.format("hive").mode("append").partitionBy("month_id", "day_id","hour_id").saveAsTable(hiveDatabase+".dwa_s_h_cwp_tangs")
//      RefreshUtil.refreshTangs()
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
