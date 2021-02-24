package com.gkgd.bigscreen.entity.dws;

import com.gkgd.bigscreen.entity.dwd.DataBusBean;
import scala.collection.mutable.ListBuffer;

/**
 * @ModelName
 * @Description
 * @Author zhangjinhang
 * @Date 2020/11/3 17:47
 * @Version V1.0.0
 */
public class AlarmBean extends DataBusBean {

    //围栏信息
    public String  fence_id;
    public String  fence_name;
    public String  fence_area_id;
    public Integer fence_flag;

    //违规地点儿
    public String alarm_start_time;
    public Double alarm_start_lng;
    public Double alarm_start_lat;
    public String alarm_start_address;
    public String alarm_end_time;
    public Double alarm_end_lng;
    public Double alarm_end_lat;
    public String alarm_end_address;

    //违规代码
    public String illegal_type_code;

    //扣分信息
    public String illegal_type_short_desc;
    public String illegal_type_desc;
    public Double enterprise_score;
    public Double vehicle_score;
    public Double driver_score;

    //附加信息
    public String uuid;
    public Integer scoring_year;
    public String interval_time;
    public Double interval_distance;

    //去了不该去的消纳场
    public String disposal_site_id;
    public String disposal_site_name;
    public String disposal_site_short_name;
    public String disposal_type;
    public String disposal_province_id;
    public String disposal_city_id;
    public String disposal_area_id;
    public String disposal_address;
    public Double disposal_lng;
    public Double disposal_lat;

    public String month_id;
    public String day_id;
    public String hour_id;

    public AlarmBean() {
        super();
    }

    public String getMonth_id() {
        return month_id;
    }

    public void setMonth_id(String month_id) {
        this.month_id = month_id;
    }

    public String getDay_id() {
        return day_id;
    }

    public void setDay_id(String day_id) {
        this.day_id = day_id;
    }

    public String getHour_id() {
        return hour_id;
    }

    public void setHour_id(String hour_id) {
        this.hour_id = hour_id;
    }

    public String getFence_id() {
        return fence_id;
    }

    public void setFence_id(String fence_id) {
        this.fence_id = fence_id;
    }

    public String getFence_name() {
        return fence_name;
    }

    public void setFence_name(String fence_name) {
        this.fence_name = fence_name;
    }

    public String getFence_area_id() {
        return fence_area_id;
    }

    public void setFence_area_id(String fence_area_id) {
        this.fence_area_id = fence_area_id;
    }

    public Integer getFence_flag() {
        return fence_flag;
    }

    public void setFence_flag(Integer fence_flag) {
        this.fence_flag = fence_flag;
    }

    public String getAlarm_start_time() {
        return alarm_start_time;
    }

    public void setAlarm_start_time(String alarm_start_time) {
        this.alarm_start_time = alarm_start_time;
    }

    public Double getAlarm_start_lng() {
        return alarm_start_lng;
    }

    public void setAlarm_start_lng(Double alarm_start_lng) {
        this.alarm_start_lng = alarm_start_lng;
    }

    public Double getAlarm_start_lat() {
        return alarm_start_lat;
    }

    public void setAlarm_start_lat(Double alarm_start_lat) {
        this.alarm_start_lat = alarm_start_lat;
    }

    public String getAlarm_start_address() {
        return alarm_start_address;
    }

    public void setAlarm_start_address(String alarm_start_address) {
        this.alarm_start_address = alarm_start_address;
    }

    public String getAlarm_end_time() {
        return alarm_end_time;
    }

    public void setAlarm_end_time(String alarm_end_time) {
        this.alarm_end_time = alarm_end_time;
    }

    public Double getAlarm_end_lng() {
        return alarm_end_lng;
    }

    public void setAlarm_end_lng(Double alarm_end_lng) {
        this.alarm_end_lng = alarm_end_lng;
    }

    public Double getAlarm_end_lat() {
        return alarm_end_lat;
    }

    public void setAlarm_end_lat(Double alarm_end_lat) {
        this.alarm_end_lat = alarm_end_lat;
    }

    public String getAlarm_end_address() {
        return alarm_end_address;
    }

    public void setAlarm_end_address(String alarm_end_address) {
        this.alarm_end_address = alarm_end_address;
    }

    public String getIllegal_type_code() {
        return illegal_type_code;
    }

    public void setIllegal_type_code(String illegal_type_code) {
        this.illegal_type_code = illegal_type_code;
    }

    public String getIllegal_type_short_desc() {
        return illegal_type_short_desc;
    }

    public void setIllegal_type_short_desc(String illegal_type_short_desc) {
        this.illegal_type_short_desc = illegal_type_short_desc;
    }

    public String getIllegal_type_desc() {
        return illegal_type_desc;
    }

    public void setIllegal_type_desc(String illegal_type_desc) {
        this.illegal_type_desc = illegal_type_desc;
    }

    public Double getEnterprise_score() {
        return enterprise_score;
    }

    public void setEnterprise_score(Double enterprise_score) {
        this.enterprise_score = enterprise_score;
    }

    public Double getVehicle_score() {
        return vehicle_score;
    }

    public void setVehicle_score(Double vehicle_score) {
        this.vehicle_score = vehicle_score;
    }

    public Double getDriver_score() {
        return driver_score;
    }

    public void setDriver_score(Double driver_score) {
        this.driver_score = driver_score;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public Integer getScoring_year() {
        return scoring_year;
    }

    public void setScoring_year(Integer scoring_year) {
        this.scoring_year = scoring_year;
    }

    public String getInterval_time() {
        return interval_time;
    }

    public void setInterval_time(String interval_time) {
        this.interval_time = interval_time;
    }

    public Double getInterval_distance() {
        return interval_distance;
    }

    public void setInterval_distance(Double interval_distance) {
        this.interval_distance = interval_distance;
    }

    public String getDisposal_site_id() {
        return disposal_site_id;
    }

    public void setDisposal_site_id(String disposal_site_id) {
        this.disposal_site_id = disposal_site_id;
    }

    public String getDisposal_site_name() {
        return disposal_site_name;
    }

    public void setDisposal_site_name(String disposal_site_name) {
        this.disposal_site_name = disposal_site_name;
    }

    public String getDisposal_site_short_name() {
        return disposal_site_short_name;
    }

    public void setDisposal_site_short_name(String disposal_site_short_name) {
        this.disposal_site_short_name = disposal_site_short_name;
    }

    public String getDisposal_type() {
        return disposal_type;
    }

    public void setDisposal_type(String disposal_type) {
        this.disposal_type = disposal_type;
    }

    public String getDisposal_province_id() {
        return disposal_province_id;
    }

    public void setDisposal_province_id(String disposal_province_id) {
        this.disposal_province_id = disposal_province_id;
    }

    public String getDisposal_city_id() {
        return disposal_city_id;
    }

    public void setDisposal_city_id(String disposal_city_id) {
        this.disposal_city_id = disposal_city_id;
    }

    public String getDisposal_area_id() {
        return disposal_area_id;
    }

    public void setDisposal_area_id(String disposal_area_id) {
        this.disposal_area_id = disposal_area_id;
    }

    public String getDisposal_address() {
        return disposal_address;
    }

    public void setDisposal_address(String disposal_address) {
        this.disposal_address = disposal_address;
    }

    public Double getDisposal_lng() {
        return disposal_lng;
    }

    public void setDisposal_lng(Double disposal_lng) {
        this.disposal_lng = disposal_lng;
    }

    public Double getDisposal_lat() {
        return disposal_lat;
    }

    public void setDisposal_lat(Double disposal_lat) {
        this.disposal_lat = disposal_lat;
    }

    @Override
    public String toString() {
        return "AlarmBean{" +
                "fence_id='" + fence_id + '\'' +
                ", fence_name='" + fence_name + '\'' +
                ", fence_area_id='" + fence_area_id + '\'' +
                ", fence_flag=" + fence_flag +
                ", alarm_start_time='" + alarm_start_time + '\'' +
                ", alarm_start_lng=" + alarm_start_lng +
                ", alarm_start_lat=" + alarm_start_lat +
                ", alarm_start_address='" + alarm_start_address + '\'' +
                ", alarm_end_time='" + alarm_end_time + '\'' +
                ", alarm_end_lng=" + alarm_end_lng +
                ", alarm_end_lat=" + alarm_end_lat +
                ", alarm_end_address='" + alarm_end_address + '\'' +
                ", illegal_type_code='" + illegal_type_code + '\'' +
                ", illegal_type_short_desc='" + illegal_type_short_desc + '\'' +
                ", illegal_type_desc='" + illegal_type_desc + '\'' +
                ", enterprise_score=" + enterprise_score +
                ", vehicle_score=" + vehicle_score +
                ", driver_score=" + driver_score +
                ", uuid='" + uuid + '\'' +
                ", scoring_year=" + scoring_year +
                ", interval_time='" + interval_time + '\'' +
                ", interval_distance=" + interval_distance +
                ", disposal_site_id='" + disposal_site_id + '\'' +
                ", disposal_site_name='" + disposal_site_name + '\'' +
                ", disposal_site_short_name='" + disposal_site_short_name + '\'' +
                ", disposal_type='" + disposal_type + '\'' +
                ", disposal_province_id='" + disposal_province_id + '\'' +
                ", disposal_city_id='" + disposal_city_id + '\'' +
                ", disposal_area_id='" + disposal_area_id + '\'' +
                ", disposal_address='" + disposal_address + '\'' +
                ", disposal_lng=" + disposal_lng +
                ", disposal_lat=" + disposal_lat +
                '}';
    }
}
