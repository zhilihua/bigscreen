package com.gkgd.bigscreen.entity.ods;

import com.gkgd.bigscreen.jt8080.BitOperater;
import com.gkgd.bigscreen.jt8080.LocationInformationReport;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

public class TblPosinfo implements Serializable {

    public String devid;  //设备id

    public String time;   //设备产生时间

    public Double lng;   //经度

    public Double lat;   //纬度

    public String point;

    public String speed;  //速度

    public String pos;   //方向 0-360

    public String gpsflag;  //定位状态:1:定位有效 0:定位失效

    public String wireflag;   //天线状态（暂时不用）

    public String powerflag;  //电源状态（0、关 1、开）

    public String accflag;   //ACC状态（0、关 1、开）

    public String auxiliaryflag;  //副发动机状态 1:启动 0：未启动

    public String t;   //温度（暂时不用）

    public String door;  //门状态（暂时不用）

    public String alldistance;   //总里程（仪表中的里程）（暂时不用）

    public String alldistfuel;   //不用

    public String gpsdistance;   //GPS里程（使用）

    public String gpsalldistance;   //gps清零后里程（暂时不用）

    public String createdatetime;   //上报时间

    public String zxcqi;    //??

    public String alarmflag;   //1:报警 0:未报警

    public String mainoilper;   //主油箱百分比

    public String residualoil;   //主发剩余油量

    public String viceoilper;   //副油箱百分比

    public String viceresidualoil;  //副发动机剩余油量(升，小数点后两位)

    public String oildifference;   //油量差

    public String sharpturn;   //急转弯

    public String brake;   //急刹车

    public static BitOperater bitOperater = new BitOperater();

    public TblPosinfo() {
    }

    public TblPosinfo(String devid, LocationInformationReport locationInformationReport) {
        this.devid = devid;
        try {
            Date dateTime = DateUtils.parseDate(locationInformationReport.getDateTime(), new String[]{"yyMMddHHmmss"});
            this.time = DateFormatUtils.format(dateTime, "yyyy-MM-dd HH:mm:ss");
        } catch (ParseException e) {
        }
        this.lng = Long.parseLong(locationInformationReport.getLongitude(), 16) / Math.pow(10, 6);
        this.lat = Long.parseLong(locationInformationReport.getLatitude(), 16) / Math.pow(10, 6);
        this.speed = String.valueOf(Long.parseLong(locationInformationReport.getSpeed(), 16) / 10D);
        this.pos = String.valueOf(Long.parseLong(locationInformationReport.getDirection(), 16));
        this.gpsflag = String.valueOf(locationInformationReport.getStateInfo().getLocation());
        this.accflag = String.valueOf(locationInformationReport.getStateInfo().getAcc());
//		this.alldistance="0";
//		this.alldistfuel="0";
        Map<Integer, LocationInformationReport.AdditionalInformation> map = locationInformationReport.getAdditionalInformationMap();
//        try {
//            if (map.containsKey(0x25)) {
//                String str = BitOperater.bin2HexStr(map.get(0x25).getInformation());
//                if(str!=null && !str.startsWith("01040"))
//                    this.auxiliaryflag = String.valueOf(bitOperater.getBitAt(Long.parseLong(str, 16), 8));
//                else {
//                    this.auxiliaryflag = "0";
//                }
//            } else {
//                this.auxiliaryflag = "0";
//            }
//        } catch (Exception e) {
//            this.auxiliaryflag = "0";
//        }
        this.auxiliaryflag = "0";
        try {
            this.gpsdistance = String.format("%.2f", Long.parseLong(BitOperater.bin2HexStr(map.get(0x01).getInformation()), 16) / 10D);
        }catch (Exception e){
//            logger.error("gpsdistance 解析错误", e);
            this.gpsdistance = "0";
        }
//		this.gpsalldistance="0";
        this.createdatetime = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
//		this.zxcqi=locationInformationReport.get;
//		this.alarmflag=locationInformationReport.get;
        if (map.containsKey(0x02)) {
            try {
                this.mainoilper = String.valueOf(Long.parseLong(BitOperater.bin2HexStr(map.get(0x02).getInformation()), 16) / 10D);
            } catch (Exception e){
//                logger.error("mainoilper 解析错误", e);
                this.mainoilper = "";
            }
        }
//		this.residualoil=locationInformationReport.get;//主发剩余油量需要计算
//		this.viceoilper=locationInformationReport.get;
//		this.viceresidualoil=locationInformationReport.get;
//		this.oildifference=locationInformationReport.get;
//		this.lastresidualoil=locationInformationReport.get;
        this.brake = String.valueOf(locationInformationReport.getAlarmInfo().getBrake());//急刹车
        this.sharpturn = String.valueOf(locationInformationReport.getAlarmInfo().getSharpturn());//急转弯
    }


    public String getDevid() {
        return devid;
    }

    public void setDevid(String devid) {
        this.devid = devid;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Double getLng() {
        return lng;
    }

    public void setLng(Double lng) {
        this.lng = lng;
    }

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    public String getPoint() {
        return this.lat + "," + this.lng;
    }

    public void setPoint(String point) {
        this.point = point;
    }

    public String getSpeed() {
        return speed;
    }

    public void setSpeed(String speed) {
        this.speed = speed;
    }

    public String getPos() {
        return pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }

    public String getGpsflag() {
        return gpsflag;
    }

    public void setGpsflag(String gpsflag) {
        this.gpsflag = gpsflag;
    }

    public String getWireflag() {
        return wireflag;
    }

    public void setWireflag(String wireflag) {
        this.wireflag = wireflag;
    }

    public String getPowerflag() {
        return powerflag;
    }

    public void setPowerflag(String powerflag) {
        this.powerflag = powerflag;
    }

    public String getAccflag() {
        return accflag;
    }

    public void setAccflag(String accflag) {
        this.accflag = accflag;
    }

    public String getAuxiliaryflag() {
        return auxiliaryflag;
    }

    public void setAuxiliaryflag(String auxiliaryflag) {
        this.auxiliaryflag = auxiliaryflag;
    }

    public String getT() {
        return t;
    }

    public void setT(String t) {
        this.t = t;
    }

    public String getDoor() {
        return door;
    }

    public void setDoor(String door) {
        this.door = door;
    }

    public String getAlldistance() {
        return alldistance;
    }

    public void setAlldistance(String alldistance) {
        this.alldistance = alldistance;
    }

    public String getAlldistfuel() {
        return alldistfuel;
    }

    public void setAlldistfuel(String alldistfuel) {
        this.alldistfuel = alldistfuel;
    }

    public String getGpsdistance() {
        return gpsdistance;
    }

    public void setGpsdistance(String gpsdistance) {
        this.gpsdistance = gpsdistance;
    }

    public String getGpsalldistance() {
        return gpsalldistance;
    }

    public void setGpsalldistance(String gpsalldistance) {
        this.gpsalldistance = gpsalldistance;
    }

    public String getCreatedatetime() {
        return createdatetime;
    }

    public void setCreatedatetime(String createdatetime) {
        this.createdatetime = createdatetime;
    }

    public String getZxcqi() {
        return zxcqi;
    }

    public void setZxcqi(String zxcqi) {
        this.zxcqi = zxcqi;
    }

    public String getAlarmflag() {
        return alarmflag;
    }

    public void setAlarmflag(String alarmflag) {
        this.alarmflag = alarmflag;
    }

    public String getMainoilper() {
        return mainoilper;
    }

    public void setMainoilper(String mainoilper) {
        this.mainoilper = mainoilper;
    }

    public String getResidualoil() {
        return residualoil;
    }

    public void setResidualoil(String residualoil) {
        this.residualoil = residualoil;
    }

    public String getViceoilper() {
        return viceoilper;
    }

    public void setViceoilper(String viceoilper) {
        this.viceoilper = viceoilper;
    }

    public String getViceresidualoil() {
        return viceresidualoil;
    }

    public void setViceresidualoil(String viceresidualoil) {
        this.viceresidualoil = viceresidualoil;
    }

    public String getOildifference() {
        return oildifference;
    }

    public void setOildifference(String oildifference) {
        this.oildifference = oildifference;
    }

    public String getSharpturn() {
        return sharpturn;
    }

    public void setSharpturn(String sharpturn) {
        this.sharpturn = sharpturn;
    }

    public String getBrake() {
        return brake;
    }

    public void setBrake(String brake) {
        this.brake = brake;
    }

}
