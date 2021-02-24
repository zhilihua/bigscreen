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
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.gavaghan.geodesy.Ellipsoid

import scala.collection.mutable.ListBuffer

object DwdRealtimeIllegalDisposalAlarm {
    //定时时间
    private var definiteTime: LocalDateTime = null    //当当前时间超过该时间时候该时间加1

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("illegalDisposal")
            //            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()

        val sparkConf = spark.sparkContext

        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val properties: Properties = PropertiesUtil.load("config.properties")
        val topic = properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_BUS)
        val groupId = "illegalDisposal"

        //获取广播变量并计算定时时间
        var data= cardAndDisposalList.getInstance(sparkConf, spark)
        var cardInfo = data._1
        var disposalInfo = data._2
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
            val valCard = cardInfo.value
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
        }

        trackAndDisposalInfos.foreachRDD(rdd => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            //定期更新广播变量(如果当前时间大于定时时间更新广播变量)
            if(LocalDateTime.now().isAfter(definiteTime)){
                definiteTime=LocalDateTime.now().plusHours(1)   //重新计算下次时间
                cardAndDisposalList.updateBroadCastVar(sparkConf, blocking = true, spark)  //更新广播变量
                data= cardAndDisposalList.getInstance(sparkConf, spark)
                cardInfo = data._1
                disposalInfo = data._2
            }
        })


        ssc.start()
        ssc.awaitTermination()
    }
}

object cardAndDisposalList{
    private val properties: Properties = PropertiesUtil.load("config.properties")
    private val host: String = properties.getProperty(ConfigConstant.MYSQL_HOST)
    private val port: String = properties.getProperty(ConfigConstant.MYSQL_PORT)
    private val user: String = properties.getProperty(ConfigConstant.MYSQL_USER)
    private val passwd: String = properties.getProperty(ConfigConstant.MYSQL_PASSWD)
    private val db: String = properties.getProperty(ConfigConstant.MYSQL_DB)
    //动态更新广播变量
    @volatile private var cardInfo: Broadcast[Array[List[Any]]] = null     //处置证
    @volatile private var disposalInfo: Broadcast[Array[List[Any]]] = null     //消纳场

    //获取广播变量单例对象
    def getInstance(sc: SparkContext, spark: SparkSession): (Broadcast[Array[List[Any]]], Broadcast[Array[List[Any]]]) = {
        if (cardInfo == null && disposalInfo == null) {
            synchronized {
                if (cardInfo == null && disposalInfo == null) {
                    val tuples = fetchLastData(spark)
                    cardInfo = sc.broadcast(tuples._1)
                    disposalInfo = sc.broadcast(tuples._2)
                }
            }
        }
        (cardInfo, disposalInfo)
    }

    //加载要广播的数据，并更新广播变量
    def updateBroadCastVar(sc: SparkContext, blocking: Boolean = false, spark: SparkSession): Unit = {
        if (cardInfo != null && disposalInfo != null) {
            //删除缓存在executors上的广播副本，并可选择是否在删除完成后进行block等待
            //底层可选择是否将driver端的广播副本也删除
            cardInfo.unpersist(blocking)
            disposalInfo.unpersist(blocking)


            val tuples = fetchLastData(spark)
            cardInfo = sc.broadcast(tuples._1)
            disposalInfo = sc.broadcast(tuples._2)
        }
    }

    def fetchLastData(spark: SparkSession) = {
        //动态获取需要更新的数据
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
        df1.createOrReplaceTempView("ods_cwp_vehicle_register_card")
        df2.createOrReplaceTempView("dim_cwp_d_disposal_site_info")

        val sql =
            """
              |select a.vehicle_id, b.disposal_site_id as disposalSiteId, b.disposal_site_name as disposalSiteName,
              |b.disposal_type as disposalType,
              |b.lng as disposalSiteLng, b.lat as disposalSiteLat
              |from ods_cwp_vehicle_register_card a
              |left join dim_cwp_d_disposal_site_info b
              |on a.disposal_site_id=b.disposal_site_id where a.state='0'
              |and b.is_delete=0 and b.audit_state='1'
              |""".stripMargin

        val sql2 =
            """
              |select lng, lat, disposal_site_id, disposal_site_name, disposal_site_short_name,
              |disposal_type, province_id, city_id, area_id, address
              |from dim_cwp_d_disposal_site_info
              |""".stripMargin
        //处置证信息
        val result = spark.sql(sql).collect().map(row => {
            row.toSeq.toList
        })

        //消纳场信息
        val disposal = spark.sql(sql2).collect().map {
            row => {
                row.toSeq.toList
            }
        }

        (result, disposal)

    }
}
