package com.gkgd.bigscreen.construction.batch

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.gkgd.bigscreen.constant.{ConfigConstant, KafkaConstant}
import com.gkgd.bigscreen.util.{MysqlUtil, PropertiesUtil, RefreshUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ModelName
  * @Description
  * @Author zhangjinhang
  * @Date 2020/11/26 18:35
  * @Version V1.0.0
  */
object AdsBuildDisposalEarthBatch {
  def main(args: Array[String]): Unit = {

    val properties: Properties = PropertiesUtil.load("config.properties")
    val hiveMetastore = properties.getProperty(ConfigConstant.HIVE_METASTORE_URIS)
    val hiveDatabase = properties.getProperty(ConfigConstant.HIVE_DATABASE)
    val spark = SparkSession.builder()
      .appName("AdsBuildDisposalEarthBatch")
//      .master("local[*]")
      .config("hive.metastore.uris", hiveMetastore)
//      .config("spark.sql.warehouse.dir", "http://192.168.10.20:8080/#/main/dashboard/metrics")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    val split_str = date.split(" ")
    val split_1 = split_str(0).split("-")
    val yearz_month = split_1(0) + "-" + split_1(1)
    val calendar = Calendar.getInstance()
    val dayz = calendar.get(Calendar.DATE).toString
    calendar.add(Calendar.DATE, -1)
    val beforeNow1 = calendar.get(Calendar.DATE).toString
    val beforeNow = dateFormat.format(calendar.getTime())
    val beforeNow1_split_str = beforeNow.split(" ")
    val beforeNow1_split_str_split_1 = beforeNow1_split_str(0).split("-")
    val before_yearz_month = beforeNow1_split_str_split_1(0) + "-" + beforeNow1_split_str_split_1(1)

    //趟数表
    spark.sql("use "+hiveDatabase)

    val tangsDF = spark.sql(s"select * from dwa_s_h_cwp_tangs where month_id='$before_yearz_month' and day_id='$beforeNow1'")  //$beforeNow1
//    val tangsDF = spark.sql(s"select * from dwa_s_h_cwp_tangs")  //$beforeNow1
    tangsDF.cache()

    val deptDF = spark.read.format("jdbc").options(MysqlUtil.getJdbcParams("dim_cwp_d_area_info")).load()
    deptDF.createOrReplaceTempView("view_dept")

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val build_erea_id1DF = tangsDF.where("s_site_flag=1 or s_site_flag=3")
      .groupBy($"s_site_province_id",$"s_site_city_id",$"s_site_area_id",$"s_site_id",$"s_site_name",$"dept_id").agg(("load","sum"),("load","count")).withColumnRenamed("sum(load)","build_sum").withColumnRenamed("count(load)","build_count").withColumn("disposal_sum",$"build_sum"*0).withColumn("disposal_count",$"build_count"*0)
      .withColumnRenamed("s_site_province_id","area_id1").withColumnRenamed("s_site_city_id","area_id2").withColumnRenamed("s_site_area_id","area_id3").withColumnRenamed("s_site_id","site_id").withColumnRenamed("s_site_name","site_name")
    build_erea_id1DF.createOrReplaceTempView("build")

    val disposal_erea_id1DF = tangsDF.where("s_site_flag=2 or s_site_flag=4")
      .groupBy($"e_site_province_id",$"e_site_city_id",$"e_site_area_id",$"e_site_id",$"e_site_name",$"dept_id").agg(("load","sum"),("load","count")).withColumnRenamed("sum(load)","disposal_sum").withColumnRenamed("count(load)","disposal_count").withColumn("build_sum",round($"disposal_sum"*0,1)).withColumn("build_count",$"disposal_count"*0)
      .withColumnRenamed("e_site_province_id","area_id1").withColumnRenamed("e_site_city_id","area_id2").withColumnRenamed("e_site_area_id","area_id3").withColumnRenamed("e_site_id","site_id").withColumnRenamed("e_site_name","site_name")
      .select($"area_id1",$"area_id2",$"area_id3",$"site_id",$"site_name",$"dept_id",$"build_sum",$"build_count",$"disposal_sum",$"disposal_count")
    disposal_erea_id1DF.createOrReplaceTempView("dispo")

    val nowDF = build_erea_id1DF.union(disposal_erea_id1DF)//.repartition(1)
    nowDF.cache()
    nowDF.createOrReplaceTempView("result_view")

    spark.sql(s"insert overwrite table dwd_d_cwp_build_disposal_statistics partition(month_id='$before_yearz_month',day_id=$beforeNow1) select * from result_view")

    val sql_1 = "select area_id1 as site_id,sum(build_sum) as build_sum,sum(build_count) as build_count,sum(disposal_sum) as disposal_sum,sum(disposal_count) as disposal_count,dept_id from result_view where area_id1 is not null and area_id1!='' group by area_id1,dept_id"
    val area_id1_df = spark.sql(sql_1)
    val sql_2 = "select area_id2 as site_id,sum(build_sum) as build_sum,sum(build_count) as build_count,sum(disposal_sum) as disposal_sum,sum(disposal_count) as disposal_count,dept_id from result_view where area_id2 is not null and area_id2!='' group by area_id2,dept_id "
    val area_id2_df = spark.sql(sql_2)
    val sql_3 = "select area_id3 as site_id,sum(build_sum) as build_sum,sum(build_count) as build_count,sum(disposal_sum) as disposal_sum,sum(disposal_count) as disposal_count,dept_id from result_view where area_id3 is not null and area_id3!='' group by area_id3,dept_id "

    val area_id3_df = spark.sql(sql_3)
    val area_df = area_id1_df.union(area_id2_df).union(area_id3_df)
    area_df.createOrReplaceTempView("area_view")
    val sql_ = "select site_id,build_sum,build_count,disposal_sum,disposal_count,view_dept.area_name as site_name,dept_id from area_view left join view_dept on area_view.site_id=view_dept.id"
    val rsDF = spark.sql(sql_)

    val sql_site = "select site_id,sum(build_sum) as build_sum,sum(build_count) as build_count,sum(disposal_sum) as disposal_sum,sum(disposal_count) as disposal_count,site_name,dept_id from result_view group by site_id,site_name,dept_id"
    val sql_site_df = rsDF.union(spark.sql(sql_site))

    val beforeDF: DataFrame = spark.read.table("dwd_d_cwp_build_disposal_amount").where(s"month_id='$before_yearz_month' and day_id=$beforeNow1").drop("month_id","day_id")
//    val beforeDF = HiveUtils.getDFFromHiveData(spark,tableName = "dwd_d_cwp_build_disposal_amount",ip = hiveUrl.split(":")(0),port = hiveUrl.split(":")(1),database = hiveDatabase,metaStoreURI = hiveMetastoreUri,userName = hiveUser,password = hivePasswd).where(s"month_id='$before_yearz_month' and day_id=$beforeNow1").drop("month_id","day_id")

    val all = sql_site_df.union(beforeDF)

    val resultDF = all.groupBy($"site_id",$"site_name",$"dept_id").agg(("build_sum","sum"),("build_count","sum"),("disposal_sum","sum"),("disposal_count","sum"))
      .withColumnRenamed("sum(build_sum)","build_sum")
      .withColumnRenamed("sum(build_count)","build_count")
      .withColumnRenamed("sum(disposal_sum)","disposal_sum")
      .withColumnRenamed("sum(disposal_count)","disposal_count")
      .select("site_id","build_sum","build_count","disposal_sum","disposal_count","site_name","dept_id")

    resultDF.createOrReplaceTempView("view_res")
    spark.sql(s"insert overwrite table dwd_d_cwp_build_disposal_amount partition(month_id='$yearz_month',day_id=$dayz) select site_id,build_sum,build_count,disposal_sum,disposal_count,site_name,dept_id from view_res")

    RefreshUtil.refreshBuildDisposal()
  }
}
