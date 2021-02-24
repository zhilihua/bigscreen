package com.gkgd.bigscreen.construction.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.gkgd.bigscreen.constant.ConfigConstant;
import com.gkgd.bigscreen.constant.KafkaConstant;
import com.gkgd.bigscreen.entity.dws.AlarmBean;
import com.gkgd.bigscreen.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.gavaghan.geodesy.Ellipsoid;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ModelName
 * @Description
 * @Author zhangjinhang
 * @Date 2020/12/3 16:50
 * @Version V1.0.0
 */
public class DwsAlarm2KafkaHiveStream {
    public static void main(String[] args) throws Exception {

        Properties properties = PropertiesUtil.load("config.properties");
        String hiveMetastore = properties.getProperty(ConfigConstant.HIVE_METASTORE_URIS);
        String kafkaServers = properties.getProperty(ConfigConstant.KAFKA_BOOTSTRAP_SERVERS);
        String dwdAlamTopic = properties.getProperty(KafkaConstant.TOPIC_DWD_DATA_ALARM);
        String dwsAlamTopic = properties.getProperty(KafkaConstant.TOPIC_DWS_DATA_ALARM);
        String hiveDatabase = properties.getProperty(ConfigConstant.HIVE_DATABASE);
        SparkConf conf = new SparkConf()
                .setAppName("HiveWrite")
//                .setMaster("local[2]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        SparkSession sparkSession = SparkSession.builder().config(conf)
                .config("hive.metastore.uris", hiveMetastore)
//                .config("spark.sql.warehouse.dir", "http://192.168.10.20:8080/#/main/dashboard/metrics")
                .config("hive.exec.dynamic.partition.mode","nonstrict")
                .enableHiveSupport()
                .getOrCreate();

        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaServers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);

        kafkaParams.put("group.id", "fweryjgegerjr");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // 构建topic set
        String[] kafkaTopicsSplited = dwdAlamTopic.split(",");

        Collection<String> topics = new HashSet<>();
        for (String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                    KafkaUtils.createDirectStream(
                            jssc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                    );

        JavaDStream<AlarmBean> alarmBeanJavaDStream = stream.map(new Function<ConsumerRecord<String, String>, AlarmBean>() {
            @Override
            public AlarmBean call(ConsumerRecord<String, String> record) throws Exception {
                String value = record.value();
                AlarmBean alarmBean = JSON.parseObject(value, AlarmBean.class);

                Date now = new Date();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String date = dateFormat.format(now);
                Calendar calendar = Calendar.getInstance();
                String dayz = String.valueOf(calendar.get(Calendar.DATE));
                String hourz = String.valueOf(calendar.get(Calendar.HOUR_OF_DAY));
                String[] split_str = date.split(" ");
                String[] split_1 = split_str[0].split("-");
                String yearz_month = split_1[0] + "-" + split_1[1];
                alarmBean.setMonth_id(yearz_month);
                alarmBean.setDay_id(dayz);
                alarmBean.setHour_id(hourz);

                String startTime = alarmBean.getAlarm_start_time();
                String endTime = alarmBean.getAlarm_end_time();

                if(startTime!=null && endTime!=null){
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Long alarmStartTime = simpleDateFormat.parse(startTime).getTime();
                    Long alarmEndTime = simpleDateFormat.parse(endTime).getTime();
                    Long diff = alarmEndTime - alarmStartTime; //毫秒级差值
                    Long minute = diff / 1000 / 60;
                    Long hour = minute / 60;
                    if(hour>=1){
                        minute = (diff / 1000) % 60;
                        alarmBean.setInterval_time(hour+"小时"+minute+"分");
                    }else {
                        alarmBean.setInterval_time(minute+"分");
                    }
                }
                Double alarmStartLng = alarmBean.getAlarm_start_lng();
                Double alarmStartLat = alarmBean.getAlarm_start_lat();
                Double alarmEndLng = alarmBean.getAlarm_end_lng();
                Double alarmEndLat = alarmBean.getAlarm_end_lat();

                if(alarmStartLng!=null && alarmStartLat!=null && alarmEndLng!=null && alarmEndLat!=null) {
                    double distanceMeter = GeoUtil.getDistanceMeter(alarmStartLng, alarmStartLat, alarmEndLng, alarmEndLat, Ellipsoid.Sphere);
                    alarmBean.setInterval_distance(distanceMeter);
                }

                Integer dept_id = alarmBean.getDept_id();
                String illegal_type_code = alarmBean.getIllegal_type_code();

                String sql = "select illegal_type_code,illegal_type_short_desc,illegal_type_desc,enterprise_score," +
                        "vehicle_score,driver_score from ods_cwp_credit_illegal_type_dept_set where CONCAT_WS('-',dept_id,illegal_type_code) in ('"+dept_id+"-"+illegal_type_code+"')";

                List<JSONObject> jsonObjList = MysqlUtil.queryListJava(sql);

                if(jsonObjList!=null && jsonObjList.size()>0){
                    JSONObject jsonObject = jsonObjList.get(0);
                    alarmBean.setIllegal_type_short_desc(jsonObject.getString("illegal_type_short_desc"));
                    alarmBean.setIllegal_type_desc(jsonObject.getString("illegal_type_desc"));
                    alarmBean.setEnterprise_score(jsonObject.getDouble("enterprise_score"));
                    alarmBean.setVehicle_score(jsonObject.getDouble("vehicle_score"));
                    alarmBean.setDriver_score(jsonObject.getDouble("driver_score"));
                }

                alarmBean.setUuid(UUID.randomUUID().toString());
                alarmBean.setScoring_year(Integer.valueOf(split_1[0]));

                if(endTime==null){
                    alarmBean.setAlarm_end_time(startTime);
                }

                if(alarmStartLat!=null && alarmStartLng!=null){
                    String address = GeoUtil.getAddress(alarmStartLat, alarmStartLng);
                    alarmBean.setAlarm_start_address(address);
                }

                if(alarmEndLat!=null && alarmEndLng!=null){
                    String address = GeoUtil.getAddress(alarmEndLat, alarmEndLng);
                    alarmBean.setAlarm_end_address(address);
                }

                return alarmBean;
            }
        });

        //写入Kafka
        JavaDStream<AlarmBean> send2KafkaStream = alarmBeanJavaDStream.map(new Function<AlarmBean, AlarmBean>() {
            @Override
            public AlarmBean call(AlarmBean alarmBean) throws Exception {
                String jsonString = JSON.toJSONString(alarmBean, SerializerFeature.WriteMapNullValue);
                MyKafkaSink.send(dwsAlamTopic, jsonString);
                return alarmBean;
            }
        });

        //写入Hive
        send2KafkaStream.foreachRDD(new VoidFunction<JavaRDD<AlarmBean>>() {
            @Override
            public void call(JavaRDD<AlarmBean> alarmBeanJavaRDD) throws Exception {
                Dataset<Row> dataFrame = sparkSession.createDataFrame(alarmBeanJavaRDD, AlarmBean.class);
                dataFrame.write().format("hive").mode("append").partitionBy("month_id", "day_id","hour_id").saveAsTable(hiveDatabase+".dwd_h_cwp_vehicle_alarm");
//                RefreshUtil.refreshAlarm();
            }
        });

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
