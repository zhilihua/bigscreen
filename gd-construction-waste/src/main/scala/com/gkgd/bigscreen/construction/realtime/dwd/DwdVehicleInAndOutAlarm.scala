package com.gkgd.bigscreen.construction.realtime.dwd

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.gkgd.bigscreen.constant.{ConfigConstant, KafkaConstant}
import com.gkgd.bigscreen.entity.dwd.DataBusBean
import com.gkgd.bigscreen.util.{GeoUtil, MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.gavaghan.geodesy.Ellipsoid

import scala.collection.mutable

object DwdVehicleInAndOutAlarm {
    private var definiteTime: LocalDateTime = null    //当当前时间超过该时间时候该时间加1
    private val properties: Properties = PropertiesUtil.load("dev-config.properties")
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.shuffle.file.buffer", "64")   //调节map端shuffle的缓存大小，减少溢写磁盘
            .config("spark.shuffle.io.retryWait", "15s")    //调节reduce端拉取数据等待间隔
            .config("spark.shuffle.sort.bypassMergeThreshold", "3000")  //调节SortShuffle排序操作阈值
            .config("spark.storage.memoryFraction", "0.4")   //降低cache操作的内存占比
            .getOrCreate()

        val sparkConf = spark.sparkContext

        val ssc = new StreamingContext(sparkConf, Seconds(10))

        val topic = properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_BUS)
        val groupId = "InOutAlarm"
        //设置时间记录广播变量，(车辆id -> 进入时间， 出去时间, 上次状态（1：在里面，2：在外面），进入状态锁（0：未，1锁），出去状态锁)
        val vehicleTimeout = mutable.Map[String, List[String]]()
        var timeoutBD = sparkConf.broadcast(vehicleTimeout)
        //获取消纳场数据
        var disposalInfos = sparkConf.broadcast(getDisposal(properties, spark))
        definiteTime=LocalDateTime.now().plusHours(1)

        //获取数据
        val recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
        //过滤数据
        val recordStream = recordInputStream.mapPartitions(records => {
            records.map {
                record  => {
                    val jsonString: String = record.value()
                    val dataBusBean: DataBusBean = JSON.parseObject(jsonString, classOf[DataBusBean])
                    (dataBusBean.vehicle_id, dataBusBean)    //为了防止不同分区中出现同一个车的数据，在写的过程中死锁
                }
            }.filter(
                record => {
                    "1".equals(record._2.manage_state) && "1".equals(record._2.audit_state)
                }
            )
        }).groupByKey(3).mapPartitions(rddPart => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val result = mutable.Map[String, List[String]]()
            rddPart.foreach((info: (Integer, Iterable[DataBusBean])) => {
                val beans = info._2.toList.sortBy(x => {
                    sdf.parse(x.time).getTime / 1000
                })
                for(disposalInfo <- disposalInfos.value){
                    for(vehicle <- beans){
                        val key = vehicle.vehicle_id.toString + disposalInfo(2)
                        val dis = GeoUtil.getDistanceMeter(vehicle.lng, vehicle.lat,
                            disposalInfo.head.toString.toDouble, disposalInfo(1).toString.toDouble, Ellipsoid.Sphere)
                        val timeOutFlag = timeoutBD.value.get(key)     //超时标志
                        if(dis < 500) {  //当前处于消纳场内
                            timeOutFlag match {
                                case None =>
                                    //将当前时刻计入缓存
                                    result += (key -> List[String](vehicle.time, 0.toString, 1.toString, 0.toString, 0.toString))

                                case f: Option[List[String]] =>
                                    val v = f.get    //获取上次记录数据
                                    if(v(2) == 1.toString){
                                        //上次为进入状态，判断时间是否连续3分钟，如果是锁进入状态，否保持不变
                                        val timeDiff = (sdf.parse(vehicle.time).getTime - sdf.parse(v.head).getTime) / 1000
                                        if(timeDiff > 180) {
                                            result += (key -> List[String](v.head, v(1), 1.toString, 1.toString, v(4)))
                                        }else{
                                            result += (key -> List[String](v.head, v(1), 1.toString, v(3), v(4)))
                                        }

                                    }else {
                                        //上次为外面状态，如果此刻进入为锁死状态将外出时间初始化，否则将状态初始化
                                        if(v(3) == 1.toString){
                                            result += (key -> List[String](v.head, 0.toString, 1.toString, v(3), v(4)))
                                        }else{
                                            result += (key -> List[String](vehicle.time, 0.toString, 1.toString, 0.toString, 0.toString))
                                        }

                                    }

                            }
                        }else {    //当前处于消纳场外面
                            timeOutFlag match {
                                case None =>    //什么都不做

                                case f: Option[List[String]] =>
                                    val v = f.get     //获取上次记录数据
                                    //只有上次是锁死状态才记录此次
                                    if(v(3) == 1.toString) {
                                        if(v(2) == 2.toString){   //如果上次为外面正常记录，上次在里面初始化外面数据
                                            //如果连续外面3分钟锁死外出状态
                                            val timeDiff = (sdf.parse(vehicle.time).getTime - sdf.parse(v(1)).getTime) / 1000
                                            if(timeDiff > 180) {
                                                result += (key -> List[String](v.head, v(1), 2.toString, v(3), 1.toString))
                                            }else{
                                                result += (key -> List[String](v.head, v(1), 2.toString, v(3), v(4)))
                                            }
                                        }else{
                                            result += (key -> List[String](v.head, vehicle.time, 2.toString, v(3), v(4)))
                                        }

                                    }

                            }

                        }
                    }
                }
            })

            result.toIterator
        })

        //将符号的数据进行报警处理
        recordStream.foreachRDD(rdd => {
            //每一个小时更新消纳场广播变量
            if(LocalDateTime.now().isAfter(definiteTime)){
                definiteTime=LocalDateTime.now().plusHours(1)   //重新计算下次时间
                vehicleInAndOutList.updateBroadCastVar(sparkConf, blocking = true,
                    getDisposal(properties, spark))  //更新广播变量
                disposalInfos = vehicleInAndOutList.getInstance(sparkConf,
                    getDisposal(properties, spark))
            }

            //更新车辆进出时间广播变量

        })

        ssc.start()
        ssc.awaitTermination()
    }

    def getDisposal(properties: Properties, spark: SparkSession) = {
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
            .option("dbtable", "dim_cwp_d_disposal_site_info")
            .load()
        df1.createOrReplaceTempView("dim_cwp_d_disposal_site_info")

        val sql =
            """
              |select lng, lat, disposal_site_id, disposal_site_name
              |from dim_cwp_d_disposal_site_info
              |where dept_id=2 and is_delete=0 and audit_state='1'
              |""".stripMargin

        //消纳场信息
        val disposal = spark.sql(sql).collect().map {
            row => {
                row.toSeq.toList
            }
        }

        disposal
    }
}

//广播变量的更新和获取
object vehicleInAndOutList {
    //动态更新广播变量
    @volatile private var instance: Broadcast[Array[List[Any]]] = null //车辆状态

    //获取广播变量单例对象
    def getInstance(sc: SparkContext,
                    data: Array[List[Any]]) = {
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
                           data: Array[List[Any]]): Unit = {
        if (instance != null) {
            //删除缓存在executors上的广播副本，并可选择是否在删除完成后进行block等待
            //底层可选择是否将driver端的广播副本也删除
            instance.unpersist(blocking)

            instance = sc.broadcast(data)
        }
    }
}
