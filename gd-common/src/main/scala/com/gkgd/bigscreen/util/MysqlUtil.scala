package com.gkgd.bigscreen.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.gkgd.bigscreen.constant.ConfigConstant

import scala.collection.mutable.ListBuffer
import java.util.{HashMap => JHashMap}
object MysqlUtil {

  private val properties: Properties = PropertiesUtil.load("config.properties")
  private val host: String = properties.getProperty(ConfigConstant.MYSQL_HOST)
  private val port: String = properties.getProperty(ConfigConstant.MYSQL_PORT)
  private val user: String = properties.getProperty(ConfigConstant.MYSQL_USER)
  private val passwd: String = properties.getProperty(ConfigConstant.MYSQL_PASSWD)
  private val db: String = properties.getProperty(ConfigConstant.MYSQL_DB)

  def queryListJava(sql:String):util.List[JSONObject]={
    Class.forName("com.mysql.jdbc.Driver")
    val resultList: ListBuffer[JSONObject] = new  ListBuffer[ JSONObject]()
    val conn: Connection = DriverManager.getConnection(s"jdbc:mysql://$host:$port/$db?characterEncoding=utf-8&useSSL=false",user,passwd)
    val stat: Statement = conn.createStatement
    val rs: ResultSet = stat.executeQuery(sql)
    val md: ResultSetMetaData = rs.getMetaData
    while ( rs.next ) {
      val rowData = new JSONObject()
      for (i  <-1 to md.getColumnCount  ) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList+=rowData
    }

    stat.close()
    conn.close()
    import scala.collection.JavaConverters._
    resultList.toList.asJava
  }


  def queryList(sql:String):List[JSONObject]={
    Class.forName("com.mysql.jdbc.Driver")
    val resultList: ListBuffer[JSONObject] = new  ListBuffer[ JSONObject]()
    val conn: Connection = DriverManager.getConnection(s"jdbc:mysql://$host:$port/$db?characterEncoding=utf-8&useSSL=false",user,passwd)
    val stat: Statement = conn.createStatement
    val rs: ResultSet = stat.executeQuery(sql)
    val md: ResultSetMetaData = rs.getMetaData
    while ( rs.next ) {
      val rowData = new JSONObject()
      for (i  <-1 to md.getColumnCount  ) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList+=rowData
    }

    stat.close()
    conn.close()
    resultList.toList
  }

  def getJdbcParams(table:String): JHashMap[String, String] = {

    val kp = new JHashMap[String, String]()
    kp.put("url", s"jdbc:mysql://$host:$port/$db?characterEncoding=utf-8&useSSL=false")
    kp.put("driver", "com.mysql.jdbc.Driver")
    kp.put("user", user)
    kp.put("password", passwd)
    kp.put("characterEncoding", "utf8")
    kp.put("useSSL", "false")
    kp.put("useUnicode", "true")
    kp.put("hive.resultset.use.unique.column.names", "false")
    kp.put("dbtable", table)
    kp
  }



  def main(args: Array[String]): Unit = {

   val sql =
      """
        |SELECT condition_id,speed,null as start_date,null as end_date,1 as fence_flag,dept_id from dim_cwp_boundary_condition_over_speed where dept_id in(2) and state=0
        | UNION ALL
        | select condition_id,NULL as speed,null as start_date,null as end_date,2 as fenc_flag,dept_id from dim_cwp_boundary_condition_penalty_fence where dept_id in(2) and state=0
        | UNION ALL
        | select condition_id,NULL as speed,start_date,end_date,3 as fenc_flag,dept_id from dim_cwp_boundary_condition_work_time where dept_id in(2) and state=0
        | UNION ALL
        |  select condition_id,NULL as speed,null as start_date,null as end_date,4 as fenc_flag,dept_id from dim_cwp_boundary_condition_penalty_out_fence where dept_id in(2) and state=0
        | UNION ALL
        |  select condition_id,NULL as speed,start_date,end_date,5 as fenc_flag,dept_id from dim_cwp_boundary_condition_document_control where dept_id in(2) and state=0
      """.stripMargin


//    println(sql)
//
//    val sql =s"""
//                |select condition_id,NULL as speed,start_date,end_date,3 as fenc_flag,dept_id from dim_cwp_boundary_condition_work_time where dept_id in(2) and state=0
//            """.stripMargin

    val jsonObjList: List[JSONObject] = MysqlUtil.queryList(sql)

    println(jsonObjList)
  }

}
