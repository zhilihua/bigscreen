package com.gkgd.bigscreen.construction.realtime.dwd

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.gkgd.bigscreen.constant.{ConfigConstant, KafkaConstant}
import com.gkgd.bigscreen.entity.dwd.DataBusBean
import com.gkgd.bigscreen.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream

import scala.collection.mutable

object DwdVehicleStopHistory {
    def main(args: Array[String]): Unit = {
        val properties: Properties = PropertiesUtil.load("config.properties")
        val hiveMetastore = properties.getProperty(ConfigConstant.HIVE_METASTORE_URIS)
        val hiveDatabase = properties.getProperty(ConfigConstant.HIVE_DATABASE)
        val spark = SparkSession.builder()
            .appName("HiveWrite")
//            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("hive.metastore.uris", hiveMetastore)
//            .config("spark.sql.warehouse.dir", "http://192.168.10.20:8080/#/main/dashboard/metrics")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .enableHiveSupport()
            .getOrCreate()

        val sparkConf = spark.sparkContext

        val ssc = new StreamingContext(sparkConf, Seconds(5))

        val topic = properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_BUS)
        val groupId = "writeHive0"

        //定义广播变量
        //广播变量数据来源
        val orgData = mutable.Map[String, VehicleStopHistory]()
        var instance = vehicleStopList.getInstance(sparkConf, orgData)

        //获取jt8080数据
        val recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

        val recordStream = recordInputStream.mapPartitions(records => {
            records.map {
                record  => {
                    val jsonString: String = record.value()
                    val dataBusBean: DataBusBean = JSON.parseObject(jsonString, classOf[DataBusBean])
                    dataBusBean
                }
            }
        }).mapPartitions(partRDD => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val lst = mutable.Map[String, VehicleStopHistory]()

            //获取系统时间
            val sysTime = LocalDateTime.now()

            val now = sysTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            val year = sysTime.getYear.toString
            val month = "%2d".format(sysTime.getMonthValue)
            val day = sysTime.getDayOfMonth.toString
            val hour = sysTime.getHour.toString
            val yearMonth = year + "-" + month

            partRDD.foreach(info => {
                val speed = info.speed
                val vehicleId = info.vehicle_id.toString
                //获取每个车的历史记录
                val vehicleHistoryInfo = instance.value.getOrElse(info.vehicle_id.toString, null)
                vehicleHistoryInfo match {
                    case null =>
                        //如果上次没有，只关注此时停止状态(类似初始化)
                        speed match {
                            case start if start.toDouble > 0 =>
                                //如果此刻处于运行状态，不做任何操作

                            case stop if stop.toDouble == 0 =>
                                //将停车数据进行记录
                                val stopInfo = VehicleStopHistory(
                                    info.vehicle_id.toString, info.enterprise_id, info.car_card_number,
                                    info.time, info.lng, info.lat,
                                    null, 0, 0,
                                    now, info.dept_id, info.department_id,
                                    yearMonth, day, hour)
                                lst += (vehicleId -> stopInfo)

                        }

                    case vehicleInfo =>
                        //如果有上次记录
                        speed match {
                            case start if start.toDouble > 0 =>
                                //如果上次停止后没有下线才进行记录
                                val timeDiff = (sdf.parse(now).getTime - sdf.parse(vehicleInfo.create_time).getTime) / 1000
                                if (timeDiff < 180) {
                                    //只有上次不下线才记录
                                    vehicleInfo.create_time = now
                                    vehicleInfo.start_time = info.time
                                    vehicleInfo.start_lng = info.lng
                                    vehicleInfo.start_lat = info.lat

                                    vehicleInfo.month_id = yearMonth
                                    vehicleInfo.day_id = day
                                    vehicleInfo.hour_id = hour

                                    lst += (vehicleId -> vehicleInfo)
                                }

                            case stop if stop.toDouble == 0 =>
                                //如果此刻处于停止状态，只更新系统时间
                                vehicleInfo.create_time = now

                                lst += (vehicleId -> vehicleInfo)
                        }

                }

            })

            lst.toIterator
        })

        recordStream.foreachRDD(rdd => {
            //拉取各个分区数据
            val strMap = rdd.collect
            if (strMap.length > 0) {
//                spark.createDataFrame(strMap.map(_._2)).show()
                //获取需要落地的数据
                val historyMap = strMap.map(_._2).filter(info => {
                    val key = info.start_time
                    key != null
                })

                //获取需要与广播变量合并的数据
                val combineMap = strMap.filter(info => {
                    info._2.start_time == null
                }).toMap

                //过滤掉过期的数据
                val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                val now = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                val nowTimeStamp = sdf.parse(now).getTime

                //过期的数据
                val delData = orgData.filter(info => {
                    (nowTimeStamp - sdf.parse(info._2.create_time).getTime) / 1000 > 180
                }).keys

                if(delData.nonEmpty) {
                    //减去过期数据
                    orgData --= delData
                }
                if(historyMap.length>0){
                    //减去已经记录过的数据
                    orgData --= historyMap.map(_.vehicle_id)
                }

                //合并生产新的广播变量
                orgData ++= combineMap

                if(historyMap.length > 0) {
                    //如果有数据才落地(数据为停车时间大于3分钟的)
                    val histories = historyMap.filter(info => {
                        val timeOut = (sdf.parse(info.start_time).getTime - sdf.parse(info.stop_time).getTime) / 1000
                        timeOut > 180
                    })

                    if(histories.length > 0) {
                        val df: DataFrame = spark.createDataFrame(histories)
                        //落地到hive
                        df.write.format("hive").mode("append").partitionBy("month_id", "day_id","hour_id")
                            .saveAsTable(hiveDatabase+".dwd_cwp_d_vehicle_stop_history")
                        df.show()
                    }
                }

                instance = vehicleStopList.getInstance(sparkConf, orgData)
            }else{
                //只检查过期的进行过滤
                //过滤掉过期的数据
                val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                val now = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                val nowTimeStamp = sdf.parse(now).getTime
                //过期的数据
                val delData = orgData.filter(info => {
                    (nowTimeStamp - sdf.parse(info._2.create_time).getTime) / 1000 > 180
                }).keys

                if(delData.nonEmpty) {
                    orgData --= delData
                }

                instance = vehicleStopList.getInstance(sparkConf, orgData)
            }

        })

        ssc.start()
        ssc.awaitTermination()
    }

}

case class VehicleStopHistory(
                 vehicle_id: String,
                 enterprise_id: String,
                 car_card_number: String,
                 stop_time: String,
                 stop_lng: Double,
                 stop_lat: Double,
                 var start_time: String,
                 var start_lng: Double,
                 var start_lat: Double,
                 var create_time: String,    //系统时间
                 dept_id: Int,
                 department_id: Int,
                 var month_id: String,
                 var day_id: String,
                 var hour_id: String)
//广播变量供executor使用
object vehicleStopList {
    //动态更新广播变量
    @volatile private var instance: Broadcast[mutable.Map[String, VehicleStopHistory]] = null //车辆状态

    //获取广播变量单例对象
    def getInstance(sc: SparkContext,
                    data: mutable.Map[String, VehicleStopHistory]) = {
        if (instance == null) {
            synchronized {
                if (instance == null) {
                    instance = sc.broadcast(data)
                }
            }
        }
        instance
    }

    //加载要广播的数据，并更新广播变量
    def updateBroadCastVar(sc: SparkContext,
                           blocking: Boolean = false,
                           data: mutable.Map[String, VehicleStopHistory]): Unit = {
        if (instance != null) {
            //删除缓存在executors上的广播副本，并可选择是否在删除完成后进行block等待
            //底层可选择是否将driver端的广播副本也删除
            instance.unpersist(blocking)

            instance = sc.broadcast(data)
        }
    }
}