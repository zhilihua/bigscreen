package com.gkgd.bigscreen.construction.realtime.dwd

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.gkgd.bigscreen.constant.{ConfigConstant, KafkaConstant}
import com.gkgd.bigscreen.entity.dwd.DataBusBean
import com.gkgd.bigscreen.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @ModelName
 * @Description
 * @Author zhangjinhang
 * @Date 2020/11/20 17:13
 * @Version V1.0.0
 */
object DdwVehicleRealtimeStatus {
    private val properties: Properties = PropertiesUtil.load("config.properties")
    private val host: String = properties.getProperty(ConfigConstant.MYSQL_HOST)
    private val port: String = properties.getProperty(ConfigConstant.MYSQL_PORT)
    private val user: String = properties.getProperty(ConfigConstant.MYSQL_USER)
    private val passwd: String = properties.getProperty(ConfigConstant.MYSQL_PASSWD)
    private val db: String = properties.getProperty(ConfigConstant.MYSQL_DB)

    //定时时间
    private var definiteTime: LocalDateTime = null

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("HiveWrite")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .enableHiveSupport()
            .getOrCreate()

        val sparkConf = spark.sparkContext

        val ssc = new StreamingContext(sparkConf, Seconds(5))

        val properties: Properties = PropertiesUtil.load("config.properties")
        val topic = properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_BUS)
        val groupId = "rh4h4h5"

        //获取jt8080数据
        val recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

        //定义广播变量
        //广播变量数据来源
        val orgData = mutable.Map[String, mutable.Map[String, String]]()
        var instance = vehicleStatusList.getInstance(sparkConf, orgData)
        definiteTime = LocalDateTime.now().plusMinutes(1)

        val recordStream = recordInputStream.mapPartitions(records => {
            records.map {
                record  => {
                    val jsonString: String = record.value()
                    val dataBusBean: DataBusBean = JSON.parseObject(jsonString, classOf[DataBusBean])
                    dataBusBean
                }
            }.filter(
                record => {
                    "1".equals(record.manage_state) && "1".equals(record.audit_state)
                }
            )
        }).mapPartitions(partRDD => {
            //建立数据库连接
            //获取mysql连接
            Class.forName("com.mysql.jdbc.Driver")
            val conn: Connection = DriverManager.getConnection(
                s"jdbc:mysql://$host:$port/$db?rewriteBatchedStatements=true&useSSL=false", user, passwd)
            val ps = conn.prepareStatement(getSql)
            var batchIndex = 0

            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val result = mutable.Map[String, mutable.Map[String, String]]()

            val sysTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) //系统时间

            //当天凌晨时间
            val today_start = LocalDateTime.of(LocalDate.now(), LocalTime.MIN) //当天零点
            val startTime = today_start.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

            partRDD.foreach(info => {
                //车辆所有信息
                val infoValue: mutable.Map[String, String] = scala.collection.mutable.Map(
                    "vehicle_id" -> info.vehicle_id.toString, "dept_id" -> info.dept_id.toString,
                    "terminal_id" -> info.devid, "time" -> info.time,
                    "lng" -> info.lng.toString, "lat" -> info.lat.toString, "speed" -> info.speed,
                    "direction" -> info.pos, "gprs_state" -> info.gpsflag,
                    "acc_state" -> info.accflag, "department_id" -> info.department_id.toString,
                    "vehicle_status" -> 0.toString, "online_time" -> 0.toString, "updatetime" -> sysTime,
                    "online_status" -> 0.toString
                )
                //判断在线状态
                if (info.speed.toDouble > 0) infoValue("online_status") = 1.toString

                val vehicleData = instance.value.getOrElse(info.vehicle_id.toString, null) //查看广播变量中是否有数据
                vehicleData match {
                    case null =>
                        //该数据不存在，添加上线信息
                        //(设备上报时间、在线累计时长、系统时间、车辆在线状态:0在线，1离线)
                        result += (info.vehicle_id.toString -> infoValue)
                    case values =>
                        //数据存在进行更新
                        val lastTime = values("time") //上次设备时间
                        val vehicleStatus = values("vehicle_status").toInt //上次在线状态

                        val nowTime = info.time //现在设备时间
                        //计算两次之间的差值，大于0才正常，并且上次状态为在线状态，在线时间进行累加
                        val timeDiff = (sdf.parse(nowTime).getTime - sdf.parse(lastTime).getTime) / 1000

                        if (timeDiff > 0) {
                            vehicleStatus match {
                                case 0 =>
                                    //上次在线
                                    val nowMinusStart = (sdf.parse(nowTime).getTime - sdf.parse(startTime).getTime) / 1000

                                    //更新在线时长，因为本次与上次状态一样，所有只更新在线时长和上次在线时间
                                    var onlineTime = values("online_time").toLong + timeDiff
                                    if (onlineTime > nowMinusStart) {
                                        onlineTime = 0L
                                    }
                                    infoValue("online_time") = onlineTime.toString

                                    //将结果存入缓存
                                    result += (info.vehicle_id.toString -> infoValue)
                                case 1 =>
                                    //上次不在线
                                    //因为上次不在线，此刻为在线，所以不需要更新累计时长
                                    infoValue("online_time") = values("online_time")
                                    result += (info.vehicle_id.toString -> infoValue)
                            }
                        }
                }
                //将在线数据写入mysql
                ps.setString(1, info.devid)
                ps.setString(2, info.time)
                ps.setDouble(3, info.lng)
                ps.setDouble(4, info.lat)
                ps.setDouble(5, info.speed.toDouble)
                ps.setInt(6, info.pos.toInt)
                ps.setInt(7, info.gpsflag.toInt)
                ps.setInt(8, info.accflag.toInt)
                ps.setString(9, info.residualoil)
                ps.setString(10, info.viceresidualoil)
                ps.setString(11, info.region_id)
                ps.setInt(12, info.department_id)
                ps.setString(13, sysTime)
                ps.setInt(14, infoValue("vehicle_status").toInt)
                ps.setInt(15, infoValue("online_time").toInt)
                ps.setDouble(16, infoValue("online_status").toDouble)

                ps.setInt(17, info.vehicle_id)
                ps.setInt(18, info.dept_id)
                ps.addBatch()

                batchIndex += 1
                // 控制提交的数量,
                if (batchIndex % 100 == 0 && batchIndex != 0) {
                    ps.executeBatch()
                    ps.clearBatch()
                }

            })

            ps.executeBatch()

            ps.close()
            conn.close()

            //转换为Iterator
            result.toIterator
        })

        recordStream.foreachRDD(rdd => {
            //拉取各个executor上的在线车辆数据合并到初始数据表
            val finalMap = rdd.collect().toMap
            //将拉取数据与原始数据合并
            orgData ++= finalMap
            //定期更新广播变量(如果当前时间大于定时时间更新广播变量)
            //获取当前系统时间

            vehicleStatusList.updateBroadCastVar(sparkConf, blocking = true, orgData) //更新广播变量
            instance = vehicleStatusList.getInstance(sparkConf, orgData)

        })

        ssc.start()
        ssc.awaitTermination()
    }

    def getSql = {
        """
          |update dwd_h_cwp_vehicle_state_card_info set
          |terminal_id=?, time=?, lng=?, lat=?, speed=?,
          |direction=?, gprs_state=?, acc_state=?, main_oil_consumption=?,
          |vice_oil_consumption=?, vehicle_area=?, department_id=?,
          |updatetime=?,vehicle_status=?,online_time=?,online_status=?
          |where vehicle_id=? and dept_id=?
          |""".stripMargin
    }

}

//广播变量供executor使用
object vehicleStatusList {
    //动态更新广播变量
    @volatile private var instance: Broadcast[mutable.Map[String, mutable.Map[String, String]]] = null //车辆状态

    //获取广播变量单例对象
    def getInstance(sc: SparkContext,
                    data: mutable.Map[String, mutable.Map[String, String]]): Broadcast[mutable.Map[String, mutable.Map[String, String]]] = {
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
                           data: mutable.Map[String, mutable.Map[String, String]]): Unit = {
        if (instance != null) {
            //删除缓存在executors上的广播副本，并可选择是否在删除完成后进行block等待
            //底层可选择是否将driver端的广播副本也删除
            instance.unpersist(blocking)

            instance = sc.broadcast(data)
        }
    }
}
