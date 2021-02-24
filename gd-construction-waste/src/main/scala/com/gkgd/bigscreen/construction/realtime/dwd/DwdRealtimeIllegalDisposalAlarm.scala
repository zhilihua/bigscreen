package com.gkgd.bigscreen.construction.realtime.dwd

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.gkgd.bigscreen.constant.{ConfigConstant, KafkaConstant}
import com.gkgd.bigscreen.entity.dws.AlarmBean
import com.gkgd.bigscreen.util.{GeoUtil, MyKafkaSink, MyKafkaUtil, PropertiesUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.gavaghan.geodesy.Ellipsoid

import scala.collection.mutable.ListBuffer
import scala.collection.mutable

object DwdRealtimeIllegalDisposalAlarm {
    //定时时间
    private var definiteTime: LocalDateTime = null    //当当前时间超过该时间时候该时间加1
    val properties: Properties = PropertiesUtil.load("config.properties")

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("illegalDisposal")
//            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()

        val sparkConf = spark.sparkContext

        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val topic = properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_BUS)
        val groupId = "illegalDisposal"

        //获取广播变量并计算定时时间
        var cardAndDisposalInfo =
            IllegalDisposalList.getInstance(sparkConf, getCardAndDisposalInfo(properties, spark))

        //广播变量存储车辆报警时间
        val timeoutData = mutable.Map[String, String]()
        var vehicleFlag = sparkConf.broadcast(timeoutData)
        definiteTime=LocalDateTime.now().plusHours(1)

        //获取jt8080数据
        val recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

        val trackAndDisposalInfos = recordInputStream.mapPartitions(records => {
            records.map {
                record  => {
                    val jsonString: String = record.value()
                    val dataBusBean: AlarmBean = JSON.parseObject(jsonString, classOf[AlarmBean])
                    dataBusBean
                }
            }.filter(
                record => {
                    "1".equals(record.manage_state) && "1".equals(record.audit_state)
                }
            )
        }).mapPartitions { partition => {
            val valCard = cardAndDisposalInfo.value._1
            //获取每个轨迹的消纳场信息
            val trackAndDisposalInfo =
                        new ListBuffer[(AlarmBean, ListBuffer[Array[String]])]()

            //匹配出每一辆车的消纳场
            partition.foreach {
                case null =>

                case info =>
                    val illegalDisposals = new ListBuffer[Array[String]]()
                    for (vc <- valCard) {
                        if (info.vehicle_id == vc.head) {
                            if (vc(4) != null && vc(5) != null) {
                                val arr = new Array[String](5)
                                arr(0) = vc(1).toString
                                arr(1) = vc(2).toString
                                arr(2) = vc(3).toString
                                arr(3) = vc(4).toString
                                arr(4) = vc(5).toString
                                illegalDisposals.append(arr)
                            }
                        }
                    }
//                    println(info)
                    trackAndDisposalInfo.append((info, illegalDisposals))
                }
                trackAndDisposalInfo.toIterator
            }
        }.mapPartitions {partition => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val finalResult = mutable.Map[String, String]()
            val valDisposal = cardAndDisposalInfo.value._2   //消纳场信息
            if(partition.nonEmpty){
                partition.foreach(
                    infos => {
                        val info = infos._1
                        //                        println(info)
                        val disposals = infos._2   //所有合理消纳场信息
                        for(vd <- valDisposal) {
                            var disFlag = 0     //记录该消纳场是否是合理消纳场标志

                            //判断该消纳场是否是自己该进的消纳场
                            if(disposals.nonEmpty) {
                                for (disposal <- disposals){
                                    //计算距离（合理消纳场与所有消纳场之间的）
                                    val dis1 = GeoUtil.getDistanceMeter(disposal(3).toDouble, disposal(4).toDouble,
                                        vd.head.toString.toDouble, vd(1).toString.toDouble, Ellipsoid.Sphere)
                                    if (dis1>0) disFlag += 1
                                }
                            }

                            if(disFlag != 0) {  //该消纳场不属于该车的合理消纳场
                                //如果该消纳场不是合理消纳场，判断距离（车辆与消纳场之间）
                                val dis = GeoUtil.getDistanceMeter(info.lng,info.lat,
                                    vd.head.toString.toDouble, vd(1).toString.toDouble, Ellipsoid.Sphere)
                                if(dis<500) {  //如果距离小于500，违规消纳
                                    //将违规消纳场信息加入告警表
                                    info.disposal_lng = vd.head.toString.toDouble
                                    info.disposal_lat = vd(1).toString.toDouble
                                    info.disposal_site_id = vd(2).toString
                                    info.disposal_site_name = vd(3).toString
                                    //                                    info.disposal_site_short_name = vd(4).toString
                                    info.disposal_type = vd(5).toString
                                    info.disposal_province_id = vd(6).toString
                                    info.disposal_city_id = vd(7).toString
                                    info.disposal_area_id = vd(8).toString
                                    info.disposal_address = vd(9).toString
                                    //违规结束信息：违规时间、违规经度、违规纬度、违规地址
                                    info.alarm_end_time = info.time
                                    info.alarm_end_lng = info.lng
                                    info.alarm_end_lat = info.lat
                                    info.alarm_end_address = GeoUtil.getAddress(info.lat, info.lat)
                                    info.illegal_type_code = "0123"

                                    //拼装该车的键
                                    val key = info.vehicle_id.toString
                                    //查询该车是否报过警（2小时内）
                                    val flag = vehicleFlag.value.get(key)
                                    // 如果redis无该条数据，将该条数据写入redis，并将告警信息写入kafka;
                                    // 如果redis有该条数据且时间大于2小时，将数据写入kafka，并对redis进行修改，如果时间小于2小时，不做任何修改
                                    flag match {
                                        case None =>
                                            finalResult += (key -> info.time)
                                            val alarmJsonString: String  = JSON.toJSONString(info,SerializerFeature.WriteMapNullValue)
                                            MyKafkaSink.send(properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_ALARM), alarmJsonString)

                                        case f: Option[String] =>
                                            //查询报警时间差
                                            val timeDiff = (sdf.parse(info.time).getTime - sdf.parse(f.get).getTime) / 1000
                                            if(timeDiff > 3600*2) {
                                                finalResult += (key -> info.time)
                                                val alarmJsonString: String  = JSON.toJSONString(info,SerializerFeature.WriteMapNullValue)
                                                MyKafkaSink.send(properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_ALARM), alarmJsonString)
                                            }

                                        case _ =>
                                    }
                                }
                            }

                        }
                    }
                )
            }

            finalResult.toIterator
        }}

        //对车辆数据进行更新并重新广播
        trackAndDisposalInfos.foreachRDD((rdd: RDD[(String, String)]) => {
            //定期更新广播变量(如果当前时间大于定时时间更新广播变量)
            if(LocalDateTime.now().isAfter(definiteTime)){
                definiteTime=LocalDateTime.now().plusHours(1)   //重新计算下次时间
                IllegalDisposalList.updateBroadCastVar(sparkConf, blocking = true,
                    getCardAndDisposalInfo(properties, spark))  //更新广播变量
                cardAndDisposalInfo = IllegalDisposalList.getInstance(sparkConf,
                    getCardAndDisposalInfo(properties, spark))
            }

            //合并
            val finalRDD = rdd.collect().toMap
            timeoutData ++= finalRDD
            //更新广播变量
            vehicleFlag.unpersist(true)
            vehicleFlag = sparkConf.broadcast(timeoutData)
        })

        ssc.start()
        ssc.awaitTermination()
    }

    //从数据库获取广播变量的初始值
    def getCardAndDisposalInfo(properties: Properties, spark: SparkSession)={
        val host: String = properties.getProperty(ConfigConstant.MYSQL_HOST)
        val port: String = properties.getProperty(ConfigConstant.MYSQL_PORT)
        val user: String = properties.getProperty(ConfigConstant.MYSQL_USER)
        val passwd: String = properties.getProperty(ConfigConstant.MYSQL_PASSWD)
        val db: String = properties.getProperty(ConfigConstant.MYSQL_DB)
        val df1 = spark
            .read
            .format("jdbc")
            .option("url", s"jdbc:mysql://$host:$port/$db?useSSL=false")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("user", user)
            .option("password", passwd)
            .option("dbtable", "ods_cwp_vehicle_register_card")
            .load()
        val df2 = spark
            .read
            .format("jdbc")
            .option("url", s"jdbc:mysql://$host:$port/$db?useSSL=false")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("user", user)
            .option("password", passwd)
            .option("dbtable", "dim_cwp_d_disposal_site_info")
            .load()
        df2.createOrReplaceTempView("dim_cwp_d_disposal_site_info")
        df1.createOrReplaceTempView("ods_cwp_vehicle_register_card")

        val sql =
            """
              |select a.vehicle_id, b.disposal_site_id as disposalSiteId, b.disposal_site_name as disposalSiteName,
              |b.disposal_type as disposalType,
              |b.lng as disposalSiteLng, b.lat as disposalSiteLat,
              |b.disposal_type,b.province_id,b.city_id,b.area_id,b.address
              |from ods_cwp_vehicle_register_card a
              |left join dim_cwp_d_disposal_site_info b
              |on a.disposal_site_id=b.disposal_site_id where a.state='0'
              |and b.is_delete=0 and b.audit_state='1' and a.dept_id = 2
              |""".stripMargin

        //处置证信息
        val result = spark.sql(sql).collect().map(row => {
            row.toSeq.toList
        })

        val sql2 =
            """
              |select lng, lat, disposal_site_id, disposal_site_name, disposal_site_short_name,
              |disposal_type, province_id, city_id, area_id, address
              |from dim_cwp_d_disposal_site_info
              |where dept_id=2 and is_delete=0 and audit_state='1'
              |""".stripMargin

        //消纳场信息
        val disposal = spark.sql(sql2).collect().map {
            row => {
                row.toSeq.toList
            }
        }

        (result, disposal)
    }
}

//广播变量获取和更新
object IllegalDisposalList {
    //动态更新广播变量
    @volatile private var instance: Broadcast[(Array[List[Any]], Array[List[Any]])] = null //车辆状态

    //获取广播变量单例对象
    def getInstance(sc: SparkContext,
                    data: (Array[List[Any]], Array[List[Any]])) = {
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
                           data: (Array[List[Any]], Array[List[Any]])): Unit = {
        if (instance != null) {
            //删除缓存在executors上的广播副本，并可选择是否在删除完成后进行block等待
            //底层可选择是否将driver端的广播副本也删除
            instance.unpersist(blocking)

            instance = sc.broadcast(data)
        }
    }
}