package com.gkgd.bigscreen.construction.realtime.ods

import java.text.SimpleDateFormat
import java.util.Date

import com.gkgd.bigscreen.entity.ods.TblPosinfo
import com.gkgd.bigscreen.jt8080.{HexStringUtils, LocationInformationReport, MsgDecoderUtil, PackageData}

/**
  * @ModelName
  * @Description
  * @Author zhangjinhang
  * @Date 2021/1/7 11:23
  * @Version V1.0.0
  */
object Test {

  def main(args: Array[String]): Unit = {
    val record = "020000380647460535020a4500000000810c00030220aa0306cf27bf00420000013121011316300501040001095c030200002504000000102b0400006d5130011b31011907"
    val input: Array[Byte] = HexStringUtils.chars2Bytes(record.toCharArray)
    val packageData: PackageData = new MsgDecoderUtil().bytes2PackageData(input)
    val header: PackageData.MsgHeader = packageData.getMsgHeader
    val msgId: Int = header.getMsgId
    val locationInformationReport: LocationInformationReport = LocationInformationReport.getEntity(packageData.getMsgBodyBytes)
    val tblPosinfo = new TblPosinfo(header.getTerminalPhone, locationInformationReport)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val nowDate: String = sdf.format(new Date)
    println(nowDate == tblPosinfo.time.split(" ")(0))
    println(tblPosinfo.time)
  }

}
