package com.gkgd.bigscreen.construction.batch

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.gkgd.bigscreen.constant.ConfigConstant
import com.gkgd.bigscreen.util.{MysqlUtil, PropertiesUtil, RefreshUtil}
import org.apache.spark.sql.SparkSession

/**
  * @ModelName
  * @Description
  * @Author zhangjinhang
  * @Date 2020/11/26 18:59
  * @Version V1.0.0
  */
object AdsCreditRankBatch {

  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")
    val hiveMetastore = properties.getProperty(ConfigConstant.HIVE_METASTORE_URIS)
    val hiveDatabase = properties.getProperty(ConfigConstant.HIVE_DATABASE)
    val spark = SparkSession.builder()
      .appName("AdsBuildDisposalEarthBatch")
//      .master("local[2]")
      .config("hive.metastore.uris", hiveMetastore)
//      .config("spark.sql.warehouse.dir", "http://192.168.10.20:8080/#/main/dashboard/metrics")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("use " + hiveDatabase)

    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    val calendar = Calendar.getInstance()
    val dayz = calendar.get(Calendar.DATE).toString
    val hourz = calendar.get(Calendar.HOUR_OF_DAY).toString
    calendar.add(Calendar.DATE, -1)
    val beforeNow1 = calendar.get(Calendar.DATE).toString
    val split_str = date.split(" ")
    val dataFrame1 = split_str(0)
    val split_1 = dataFrame1.split("-")
    val yearz_month = split_1(0) + "-" + split_1(1)

    import spark.implicits._
    //违规详细表  ods_cwp_credit_illegal_detail
    val creditIllegalDetailDF = spark.read.format("jdbc").options(MysqlUtil.getJdbcParams("ods_cwp_credit_illegal_detail")).load()
    creditIllegalDetailDF.createOrReplaceTempView("view_credit_illegal_detail")

    //关联违规详细表和审核操作表
     val todayIllegalDF = creditIllegalDetailDF.select($"vehicle_id",$"car_card_number",$"enterprise_name",$"id".as("illegal_id"),$"enterprise_final_score",$"vehicle_driver_final_score",$"enterprise_id".as("transport_enterprise_id"),$"province_id",$"city_id",$"area_id",$"illegal_type_short_desc".as("illegal_name"),$"create_time".as("illegal_date"),$"create_time".as("create_date"),$"dept_id")
    todayIllegalDF.createOrReplaceTempView("view_illegalScoring")

//    //违规倾倒
//    val dumpViewDF = HiveUtils.getDFFromHiveData(spark,tableName = "dwa_s_h_cwp_tangs",ip = hiveUrl.split(":")(0),port = hiveUrl.split(":")(1),database = hiveDatabase,metaStoreURI = hiveMetastoreUri,userName = hiveUser,password = hivePasswd).where(s"month_id='$yearz_month' and day_id='$dayz'")
//    val illegal_dumpDF = dumpViewDF.where("s_site_flag=1 and e_site_flag!=2").withColumn("illegal_id",randn().cast(IntegerType)*0+10001).withColumn("enterprise_final_score",randn().cast(DoubleType)*0).withColumn("vehicle_driver_final_score",randn().cast(DoubleType)*0)
//    val resDF = illegal_dumpDF.selectExpr("vehicle_id","car_card_number","enterprise_name","illegal_id","enterprise_final_score","vehicle_driver_final_score","transport_enterprise_id","province_id","city_id","area_id","'违规倾倒' as illegal_name","updatetime as illegal_date","updatetime as create_date","dept_id")
//    val allDF = todayIllegalDF.union(resDF)

    val allDF = todayIllegalDF
    allDF.createOrReplaceTempView("view_illegal_scoring")
    spark.sql(s"insert overwrite table dwd_h_cwp_illegal_scoring_rule_detail partition(month_id='$yearz_month',day_id=$dayz) select * from view_illegal_scoring")

    /////////////////////////////////////根据类别
    val illegal_scoring_area_sql = "select province_id,city_id,area_id,illegal_id,illegal_name,count(distinct(vehicle_id)) as illegal_vehicle_count,count(*) as illegal_count,sum(vehicle_driver_final_score) as illegal_sum,dept_id from view_illegal_scoring group by dept_id,province_id,city_id,area_id,illegal_id,illegal_name"
    val illegal_scoring_area_df = spark.sql(illegal_scoring_area_sql)
    illegal_scoring_area_df.createOrReplaceTempView("view_dwd_h_cwp_scoring_rank_by_illegalType")
    spark.sql(s"insert overwrite table dwd_h_cwp_scoring_rank_by_illegalType partition(month_id='$yearz_month',day_id=$dayz) select * from view_dwd_h_cwp_scoring_rank_by_illegalType")

    ////////////////////////////////////////////////根据公司
    val illegal_scoring_enterprise_sql = "select province_id,city_id,area_id,transport_enterprise_id,enterprise_name,count(distinct(vehicle_id)) as illegal_vehicle_count,count(*) as illegal_count,sum(enterprise_final_score) as illegal_sum,dept_id from view_illegal_scoring group by dept_id,province_id,city_id,area_id,transport_enterprise_id,enterprise_name"
    val illegal_scoring_enterprise_df = spark.sql(illegal_scoring_enterprise_sql)
    illegal_scoring_enterprise_df.createOrReplaceTempView("view_dwd_h_cwp_scoring_rank_by_enterprise")
    spark.sql(s"insert overwrite table dwd_h_cwp_scoring_rank_by_enterprise partition(month_id='$yearz_month',day_id=$dayz) select * from view_dwd_h_cwp_scoring_rank_by_enterprise")

    ////////////////////////////////////////////////根据公司类别
    val illegal_scoring_enterprise_type_sql = "select province_id,city_id,area_id,transport_enterprise_id,enterprise_name,illegal_id,illegal_name,count(*) as illegal_count,sum(enterprise_final_score) as illegal_sum,dept_id from view_illegal_scoring group by province_id,city_id,area_id,transport_enterprise_id,enterprise_name,illegal_id,illegal_name,dept_id"
    val illegal_scoring_enterprise_type_df = spark.sql(illegal_scoring_enterprise_type_sql)
    illegal_scoring_enterprise_type_df.createOrReplaceTempView("view_dwd_h_cwp_scoring_rank_by_enterprise_illegalType")
    spark.sql(s"insert overwrite table dwd_h_cwp_scoring_rank_by_enterprise_illegalType partition(month_id='$yearz_month',day_id=$dayz) select * from view_dwd_h_cwp_scoring_rank_by_enterprise_illegalType")

    //////////////////////////////////////////////////////////////////根据车辆
    val illegal_scoring_vehicle_id_sql = "select province_id,city_id,area_id,vehicle_id,car_card_number,count(*) as illegal_count,sum(vehicle_driver_final_score) as illegal_sum,dept_id from view_illegal_scoring group by province_id,city_id,area_id,vehicle_id,car_card_number,dept_id"
    val illegal_scoring_vehicle_id_df = spark.sql(illegal_scoring_vehicle_id_sql)
    illegal_scoring_vehicle_id_df.createOrReplaceTempView("view_dwd_h_cwp_scoring_rank_by_vehicle")
    spark.sql(s"insert overwrite table dwd_h_cwp_scoring_rank_by_vehicle partition(month_id='$yearz_month',day_id=$dayz) select * from view_dwd_h_cwp_scoring_rank_by_vehicle")

    ///////////////////////////////////////////////////////根据车辆类别
    val illegal_scoring_vehicleID_illegalType_sql = "select province_id,city_id,area_id,vehicle_id,car_card_number,illegal_id,illegal_name,count(distinct(vehicle_id)) as illegal_vehicle_count,count(*) as illegal_count,sum(vehicle_driver_final_score) as illegal_sum,dept_id from view_illegal_scoring group by province_id,city_id,area_id,vehicle_id,car_card_number,illegal_id,illegal_name,dept_id"
    val illegal_scoring_vehicleID_illegalType_df = spark.sql(illegal_scoring_vehicleID_illegalType_sql)
    illegal_scoring_vehicleID_illegalType_df.createOrReplaceTempView("view_dwd_h_cwp_scoring_rank_by_vehicle_illegalType")
    spark.sql(s"insert overwrite table dwd_h_cwp_scoring_rank_by_vehicle_illegalType partition(month_id='$yearz_month',day_id=$dayz) select * from view_dwd_h_cwp_scoring_rank_by_vehicle_illegalType")

    //车辆
    val illegal_scoring_type_vehicle_sql = "select province_id,city_id,area_id,sum(case illegal_id when '0110' then vehicle_driver_final_score else 0 end) as alarm_speed,sum(case illegal_id when '0108' then vehicle_driver_final_score else 0 end) as alarm_out,sum(case illegal_id when '0121' then vehicle_driver_final_score else 0 end) as alarm_dump,sum(case illegal_id when '0107' then vehicle_driver_final_score else 0 end) as alarm_offline,(sum(case illegal_id when '0110' then vehicle_driver_final_score else 0 end)+sum(case illegal_id when '0108' then vehicle_driver_final_score else 0 end)+sum(case illegal_id when '0121' then vehicle_driver_final_score else 0 end)+sum(case illegal_id when '0107' then vehicle_driver_final_score else 0 end)) as total_score,dept_id from view_illegal_scoring group by province_id,city_id,area_id,dept_id"
    val illegal_scoring_type_vehicle_df = spark.sql(illegal_scoring_type_vehicle_sql)
    illegal_scoring_type_vehicle_df.createOrReplaceTempView("view_dwd_h_cwp_scoring_type_by_vehicle")
    spark.sql(s"insert overwrite table dwd_h_cwp_scoring_type_by_vehicle partition(month_id='$yearz_month',day_id=$dayz) select * from view_dwd_h_cwp_scoring_type_by_vehicle")

    //车辆id 车辆score     vehicle
    val illegal_scoring_type_vehicleID_sql = "select province_id,city_id,area_id,vehicle_id,car_card_number,transport_enterprise_id,sum(case illegal_id when '0110' then vehicle_driver_final_score else 0 end) as alarm_speed,sum(case illegal_id when '0108' then vehicle_driver_final_score else 0 end) as alarm_out,sum(case illegal_id when '0121' then vehicle_driver_final_score else 0 end) as alarm_dump,sum(case illegal_id when '0107' then vehicle_driver_final_score else 0 end) as alarm_offline,(sum(case illegal_id when '0110' then vehicle_driver_final_score else 0 end)+sum(case illegal_id when '0108' then vehicle_driver_final_score else 0 end)+sum(case illegal_id when '0121' then vehicle_driver_final_score else 0 end)+sum(case illegal_id when '0107' then vehicle_driver_final_score else 0 end)) as total_score,dept_id from view_illegal_scoring group by province_id,city_id,area_id,vehicle_id,car_card_number,transport_enterprise_id,dept_id"
    val illegal_scoring_type_vehicleID_df = spark.sql(illegal_scoring_type_vehicleID_sql)
    illegal_scoring_type_vehicleID_df.createOrReplaceTempView("view_dwd_h_cwp_scoring_type_by_vehicle_id")
    spark.sql(s"insert overwrite table dwd_h_cwp_scoring_type_by_vehicle_id partition(month_id='$yearz_month',day_id=$dayz) select * from view_dwd_h_cwp_scoring_type_by_vehicle_id")

    //公司
    val illegal_scoring_type_enterprise_sql = "select province_id,city_id,area_id,sum(case illegal_id when '0110' then enterprise_final_score else 0 end) as alarm_speed,sum(case illegal_id when '0108' then enterprise_final_score else 0 end) as alarm_out,sum(case illegal_id when '0121' then enterprise_final_score else 0 end) as alarm_dump,sum(case illegal_id when '0107' then enterprise_final_score else 0 end) as alarm_offline,(sum(case illegal_id when '0110' then enterprise_final_score else 0 end)+sum(case illegal_id when '0108' then enterprise_final_score else 0 end)+sum(case illegal_id when '0121' then enterprise_final_score else 0 end)+sum(case illegal_id when '0107' then enterprise_final_score else 0 end)) as total_score,dept_id from view_illegal_scoring group by province_id,city_id,area_id,dept_id"
    val illegal_scoring_type_enterprise_df = spark.sql(illegal_scoring_type_enterprise_sql)
    illegal_scoring_type_enterprise_df.createOrReplaceTempView("view_dwd_h_cwp_scoring_type_by_enterprise")
    spark.sql(s"insert overwrite table dwd_h_cwp_scoring_type_by_enterprise partition(month_id='$yearz_month',day_id=$dayz) select * from view_dwd_h_cwp_scoring_type_by_enterprise")

    //公司id 公司score
    val illegal_scoring_type_enterpriseID_sql = "select province_id,city_id,area_id,transport_enterprise_id,enterprise_name,sum(case illegal_id when '0110' then enterprise_final_score else 0 end) as alarm_speed,sum(case illegal_id when '0108' then enterprise_final_score else 0 end) as alarm_out,sum(case illegal_id when '0121' then enterprise_final_score else 0 end) as alarm_dump,sum(case illegal_id when '0107' then enterprise_final_score else 0 end) as alarm_offline,(sum(case illegal_id when '0110' then enterprise_final_score else 0 end)+sum(case illegal_id when '0108' then enterprise_final_score else 0 end)+sum(case illegal_id when '0121' then enterprise_final_score else 0 end)+sum(case illegal_id when '0107' then enterprise_final_score else 0 end)) as total_score,dept_id from view_illegal_scoring group by province_id,city_id,area_id,transport_enterprise_id,enterprise_name,dept_id"
    val illegal_scoring_type_enterpriseID_df = spark.sql(illegal_scoring_type_enterpriseID_sql)
    illegal_scoring_type_enterpriseID_df.createOrReplaceTempView("view_dwd_h_cwp_scoring_type_by_enterprise_id")
    spark.sql(s"insert overwrite table dwd_h_cwp_scoring_type_by_enterprise_id partition(month_id='$yearz_month',day_id=$dayz) select * from view_dwd_h_cwp_scoring_type_by_enterprise_id")

    ////////////////////////////////////////////////////////省市区汇总
    val illegal_scoring_total_sql = "select province_id,city_id,area_id,count(*) as illegal_count,sum(vehicle_driver_final_score) as illegal_sum,dept_id from view_illegal_scoring group by province_id,city_id,area_id,dept_id"
    val illegal_scoring_total_df = spark.sql(illegal_scoring_total_sql)

    illegal_scoring_total_df.createOrReplaceTempView("view_dwd_h_cwp_scoring_rank_total")
    spark.sql(s"insert overwrite table dwd_h_cwp_scoring_rank_total partition(month_id='$yearz_month',day_id=$dayz) select * from view_dwd_h_cwp_scoring_rank_total")
//    RefreshUtil.refreshCreditRank()
  }
}
