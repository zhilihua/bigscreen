package com.gkgd.bigscreen.entity.dws;

import com.gkgd.bigscreen.entity.dwd.DataBusBean;

/**
 * @ModelName  工地和消纳场公共类
 * @Description
 * @Author zhangjinhang
 * @Date 2020/11/23 14:33
 * @Version V1.0.0
 */
public class SiteBean extends DataBusBean {
    public String site_id;
    public String site_name;
    public String site_address;
    public Double site_lng;
    public Double site_lat;
    public Integer site_radius;
    public String site_province_id;
    public String site_city_id;
    public String site_area_id;
    public Integer site_flag;  //1工地  2消纳场

    public Integer work_state;  // 1进入  2工作  0离开
    public String inter_time;    //进入时间
    public String work_time;    //工作时间
    public String leave_time;   //离开时间


    public SiteBean() {
    }


    public String getSite_id() {
        return site_id;
    }

    public void setSite_id(String site_id) {
        this.site_id = site_id;
    }

    public String getSite_name() {
        return site_name;
    }

    public void setSite_name(String site_name) {
        this.site_name = site_name;
    }

    public String getSite_address() {
        return site_address;
    }

    public void setSite_address(String site_address) {
        this.site_address = site_address;
    }

    public Double getSite_lng() {
        return site_lng;
    }

    public void setSite_lng(Double site_lng) {
        this.site_lng = site_lng;
    }

    public Double getSite_lat() {
        return site_lat;
    }

    public void setSite_lat(Double site_lat) {
        this.site_lat = site_lat;
    }

    public Integer getSite_radius() {
        return site_radius;
    }

    public void setSite_radius(Integer site_radius) {
        this.site_radius = site_radius;
    }

    public String getSite_province_id() {
        return site_province_id;
    }

    public void setSite_province_id(String site_province_id) {
        this.site_province_id = site_province_id;
    }

    public String getSite_city_id() {
        return site_city_id;
    }

    public void setSite_city_id(String site_city_id) {
        this.site_city_id = site_city_id;
    }

    public String getSite_area_id() {
        return site_area_id;
    }

    public void setSite_area_id(String site_area_id) {
        this.site_area_id = site_area_id;
    }

    public Integer getSite_flag() {
        return site_flag;
    }

    public void setSite_flag(Integer site_flag) {
        this.site_flag = site_flag;
    }

    public Integer getWork_state() {
        return work_state;
    }

    public void setWork_state(Integer work_state) {
        this.work_state = work_state;
    }

    public String getInter_time() {
        return inter_time;
    }

    public void setInter_time(String inter_time) {
        this.inter_time = inter_time;
    }

    public String getWork_time() {
        return work_time;
    }

    public void setWork_time(String work_time) {
        this.work_time = work_time;
    }

    public String getLeave_time() {
        return leave_time;
    }

    public void setLeave_time(String leave_time) {
        this.leave_time = leave_time;
    }

    @Override
    public String toString() {
        return "SiteBean{" +
                "site_id='" + site_id + '\'' +
                ", site_name='" + site_name + '\'' +
                ", site_address='" + site_address + '\'' +
                ", site_lng=" + site_lng +
                ", site_lat=" + site_lat +
                ", site_radius=" + site_radius +
                ", site_province_id='" + site_province_id + '\'' +
                ", site_city_id='" + site_city_id + '\'' +
                ", site_area_id='" + site_area_id + '\'' +
                ", site_flag=" + site_flag +
                ", work_state=" + work_state +
                ", inter_time='" + inter_time + '\'' +
                ", work_time='" + work_time + '\'' +
                ", leave_time='" + leave_time + '\'' +
                '}';
    }
}
