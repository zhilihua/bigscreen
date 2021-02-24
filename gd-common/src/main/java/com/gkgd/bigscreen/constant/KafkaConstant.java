package com.gkgd.bigscreen.constant;

/**
 * @ModelName
 * @Description
 * @Author zhangjinhang
 * @Date 2020/11/19 15:12
 * @Version V1.0.0
 */
public class KafkaConstant {

    ///////////////////////////////////Topic
    //jt8080 加密数据流
    public static final String TOPIC_ODS_TRACKS = "topic.ods.tracks";
    //焦作等地区无加密数据，数据中没有devId,有车牌号
    public static final String TOPIC_ODS_TRACKS_RAW = "topic.ods.tracks.raw";
    //数据中转流
    public static final String TOPIC_DWD_DATA_ETL = "topic.dwd.data.etl";
    //数据总线流
    public static final String TOPIC_DWD_DATA_BUS = "topic.dwd.data.bus";

    //趟数数据流
    public static final String TOPIC_DWS_DATA_TANGS = "topic.dws.data.tangs";

    //告警数据流
    public static final String TOPIC_DWD_DATA_ALARM= "topic.dwd.data.alarm";

    //告警数据流
    public static final String TOPIC_DWS_DATA_ALARM= "topic.dws.data.alarm";
}
