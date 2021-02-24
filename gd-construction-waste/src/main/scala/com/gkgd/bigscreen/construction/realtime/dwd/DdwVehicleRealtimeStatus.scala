package com.gkgd.bigscreen.construction.realtime.dwd

import java.sql.{Connection, DriverManager, PreparedStatement, Statement}
import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.gkgd.bigscreen.constant.{ConfigConstant, KafkaConstant}
import com.gkgd.bigscreen.entity.dwd.DataBusBean
import com.gkgd.bigscreen.util.{MyKafkaUtil, MysqlUtil, PropertiesUtil, duridUtils}
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

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
//            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//            .config("spark.locality.wait", "6")     //调节等待时间，使shuffle尽可能发生在本节点上
            .config("spark.shuffle.file.buffer", "64")   //调节map端shuffle的缓存大小，减少溢写磁盘
            .config("spark.shuffle.io.retryWait", "15s")    //调节reduce端拉取数据等待间隔
            .config("spark.shuffle.sort.bypassMergeThreshold", "3000")  //调节SortShuffle排序操作阈值
            .config("spark.storage.memoryFraction", "0.4")   //降低cache操作的内存占比
            .getOrCreate()

        val sparkConf = spark.sparkContext

        val ssc = new StreamingContext(sparkConf, Seconds(10))

        val topic = properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_BUS)
        val groupId = "rh4h4h5"

        //获取jt8080数据
        val recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

        //定义广播变量
        //广播变量数据来源
//        val orgData = mutable.Map[String, mutable.Map[String, String]]()
        val orgData = initOrg()
        var instance = vehicleStatusList.getInstance(sparkConf, orgData)

        //获取mysql连接
        Class.forName("com.mysql.jdbc.Driver")
        val con: Connection = DriverManager.getConnection(
            s"jdbc:mysql://$host:$port/$db?rewriteBatchedStatements=true&useSSL=false", user, passwd)
        val st1: Statement = con.createStatement()     //准备写入数据库的数据

        val recordStream = recordInputStream.mapPartitions(records => {
            records.map {
                record  => {
                    val jsonString: String = record.value()
                    val dataBusBean: DataBusBean = JSON.parseObject(jsonString, classOf[DataBusBean])
                    (dataBusBean.vehicle_id, dataBusBean)    //为了防止不同分区中出现同一个车的数据，在写的过程中死锁
                }
            }
        }).groupByKey(3).mapPartitions(partRDD => {     //将相同的车加入相同的分区不会出现同时写一条数据
            //建立数据库连接
            val conn = duridUtils.getConn
            conn.setAutoCommit(false)
            val sql1 = getSql
            val ps = conn.prepareStatement(sql1)

            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val result = mutable.Map[String, mutable.Map[String, String]]()

            val sysTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) //系统时间

            //当天凌晨时间
            val today_start = LocalDateTime.of(LocalDate.now(), LocalTime.MIN) //当天零点
            val startTime = today_start.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

            partRDD.foreach(infos => {
                for(info <- infos._2) {
                //车辆所有信息（默认上线，在线行驶）
                val infoValue: mutable.Map[String, String] = scala.collection.mutable.Map(
                    "vehicle_id" -> info.vehicle_id.toString, "dept_id" -> info.dept_id.toString,
                    "terminal_id" -> info.devid, "time" -> info.time,
                    "lng" -> info.lng.toString, "lat" -> info.lat.toString, "speed" -> info.speed,
                    "direction" -> info.pos, "gprs_state" -> info.gpsflag,
                    "acc_state" -> info.accflag, "department_id" -> info.department_id.toString,
                    "vehicle_status" -> 0.toString, "online_time" -> 0.toString, "updatetime" -> sysTime,
                    "online_status" -> 1.toString, "last_zero_status_speed" -> 0.toString,
                    "main_oil_consumption" -> info.residualoil, "vice_oil_consumption" -> info.viceresidualoil,
                    "vehicle_area" -> info.region_id
                )

                val vehicleData = instance.value.getOrElse(info.vehicle_id.toString, null) //查看广播变量中是否有数据
                vehicleData match {
                    case null =>
                        //该数据不存在，添加上线信息
                        //(设备上报时间、在线累计时长、系统时间、车辆在线状态:0在线，1离线)
                        //                        infoValue("updatetime") = sysTime
                        if(info.speed.toDouble == 0) {
                            infoValue("last_zero_status_speed") = sysTime
                        }
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
                                    //更新车辆在线停止状态
                                    if(info.speed.toDouble == 0){
                                        if(values("last_zero_status_speed") != 0.toString){
                                            val timeOut = (sdf.parse(sysTime).getTime - sdf.parse(values("last_zero_status_speed")).getTime) / 1000
                                            if(timeOut >= 180) {
                                                infoValue("online_status") = 0.toString
                                                infoValue("last_zero_status_speed") = values("last_zero_status_speed")
                                            }
                                            else infoValue("last_zero_status_speed") = values("last_zero_status_speed")
                                        }else{
                                            infoValue("last_zero_status_speed")=sysTime
                                        }
                                    }
                                    //上次在线
                                    val nowMinusStart = (sdf.parse(nowTime).getTime - sdf.parse(startTime).getTime) / 1000

                                    //更新在线时长，因为本次与上次状态一样，所有只更新在线时长和上次在线时间
                                    var onlineTime = values("online_time").toLong + timeDiff
                                    if (onlineTime > nowMinusStart) {
                                        onlineTime = 0L
                                    }
                                    infoValue("online_time") = onlineTime.toString
                                    //                                    infoValue("updatetime") = sysTime

                                    //将结果存入缓存
                                    result += (info.vehicle_id.toString -> infoValue)
                                case 1 =>
                                    //更新车辆在线停止状态
                                    if(info.speed.toDouble == 0) infoValue("last_zero_status_speed") = sysTime
                                    //上次不在线
                                    //因为上次不在线，此刻为在线，所以不需要更新累计时长
                                    infoValue("online_time") = values("online_time")
                                    //                                    infoValue("updatetime") = sysTime
                                    result += (info.vehicle_id.toString -> infoValue)
                            }
                        }
                }
                //写入mysql
                updateOnline(infoValue, ps)
                }
            })

            try {
                ps.executeBatch()
                //ps.clearBatch()
                conn.commit()
            }catch {
                case e: Exception => e.printStackTrace()
            }

            ps.close()
            conn.close()
            //转换为Iterator
            result.toIterator
        }).cache()

        recordStream.foreachRDD(rdd => {
            //拉取各个executor上的在线车辆数据合并到初始数据表
            val finalMap = rdd.collect().toMap
            //将拉取数据与原始数据合并
            orgData ++= finalMap
            //遍历下线（每隔一分钟，并且更新广播变量）
            //定期更新广播变量(如果当前时间大于定时时间更新广播变量)
            //            if(LocalDateTime.now().isAfter(definiteTime)){
            //获取当前系统时间
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            //遍历下线
//            println("===============总数据量==================" + orgData.size)
            for (key <- orgData.keys) {

                if ("0".equals(orgData(key)("vehicle_status"))) {
//                    println(orgData(key)("vehicle_id")+"========================获取车辆信息===========================", orgData(key)("updatetime"), orgData(key)("time"), now)
                    val timeDiff = (sdf.parse(now).getTime - sdf.parse(orgData(key)("updatetime")).getTime) / 1000
                    if (timeDiff > 180) {
//                        println("========================开始下线车辆=============================")
                        //将该车下线
                        orgData(key)("vehicle_status") = 1.toString
                        //写入数据库
                        updateMysql(key, st1)
                    }

                }
            }
            //将结果更新到数据库
            st1.executeBatch()
            st1.clearBatch()

            //                definiteTime=LocalDateTime.now().plusMinutes(1)   //重新计算下次时间
            vehicleStatusList.updateBroadCastVar(sparkConf, blocking = true, orgData) //更新广播变量
            instance = vehicleStatusList.getInstance(sparkConf, orgData)

            //            }

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

    def updateMysql(vehicle_id: String, st: Statement) = {
        val sql = s"update dwd_h_cwp_vehicle_state_card_info set vehicle_status=1 where vehicle_id=${vehicle_id}"
        st.addBatch(sql)
    }

    def updateOnline(infoValue: mutable.Map[String, String],
                     ps: PreparedStatement) = {
        //将在线数据写入mysql
        try {
            ps.setString(1, infoValue("terminal_id"))   //替换设备id String
            ps.setString(2, infoValue("time"))   //设备时间 String
            ps.setDouble(3, infoValue("lng").toDouble)   //经度  double
            ps.setDouble(4, infoValue("lat").toDouble)   //纬度  double
            ps.setDouble(5, infoValue("speed").toDouble)    //速度  double
            ps.setInt(6, infoValue("direction").toDouble.toInt)    //int
            //对特殊字段进行处理
            if(infoValue("gprs_state") == null){    //int
                ps.setInt(7, 9)             //9代表空
            }else{
                ps.setInt(7, infoValue("gprs_state").toInt)
            }

            if(infoValue("acc_state") == null){    //int
                ps.setInt(8, 9)      //9代表空
            }else{
                ps.setInt(8, infoValue("acc_state").toInt)
            }

            if(infoValue("main_oil_consumption") == null){   //string
                ps.setString(9, null)
            }else{
                ps.setString(9, infoValue("main_oil_consumption"))
            }

            if(infoValue("vice_oil_consumption") == null){   //string
                ps.setString(10, null)
            }else{
                ps.setString(10, infoValue("vice_oil_consumption"))
            }

            if(infoValue("vehicle_area") == null){   //string
                ps.setString(11, null)
            }else{
                ps.setString(11, infoValue("vehicle_area"))
            }

            ps.setInt(12, infoValue("department_id").toInt)   //监管单位  int
            ps.setString(13, infoValue("updatetime"))       //系统时间  string
            ps.setInt(14, infoValue("vehicle_status").toInt)        //车辆上下线状态  int
            ps.setInt(15, infoValue("online_time").toInt)     //当日累计在线时长  int
            ps.setInt(16, infoValue("online_status").toInt)     //车辆在线状态  int
            ps.setInt(17, infoValue("vehicle_id").toInt)   //车辆id  int
            ps.setInt(18, infoValue("dept_id").toInt)      //机构id   int

            ps.addBatch()

        }catch {
            case e: Exception =>
                println(e)
        }
    }

    //初始化数据
    def initOrg() = {
        val sql =
            """
              |select vehicle_id, dept_id, terminal_id, time, lng, lat, speed, direction, gprs_state, acc_state,
              |     department_id, vehicle_status, online_time, updatetime, online_status, 0 as last_zero_status_speed
              |from dwd_h_cwp_vehicle_state_card_info where vehicle_status='0' and dept_id = 2
              |""".stripMargin

        val list = MysqlUtil.queryList(sql)
        val res = mutable.Map[String, mutable.Map[String, String]]()
        list.map{info => {
            import scala.collection.JavaConverters._
            val map = mutable.Map[String, String]()
            val keys = info.keySet().asScala
            for (key <- keys){
                val value = info.getString(key)
                val str = value match {
                    case "false" => 0.toString
                    case "true" => 1.toString
                    case _ => value
                }
                map.put(key, str)
            }
            res += (info.getString("vehicle_id") -> map)
        }}
        res
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