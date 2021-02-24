package com.gkgd.bigscreen.entity.dws;

/**
 * @ModelName  趟数类
 * @Description
 * @Author zhangjinhang
 * @Date 2020/11/3 11:57
 * @Version V1.0.0
 */
public class TangBean {
    public Integer vehicle_id;       //车辆id                  ------------------------->关联服务费、双向登记卡、运输证
    public String  car_card_number;     //车牌号
    public String  vehicle_model_id;    //车辆类型 16 重型自卸货车 17中型自卸货车  case when view_vehicle.vehicle_model_id=0 or view_vehicle.vehicle_model_id=16 then 19.32 when view_vehicle.vehicle_model_id=17 then 14.90 else 0 end as load
    public Float   approved_tonnage;      //核定吨位
    public Double  load;

    public Integer driver_id;         //司机ID                  ------------------------>关联司机表
    public String  driver_name;         //司机姓名

    public String  enterprise_id;      //公司ID                  ------------------------>关联公司表
    public String  enterprise_name;    //公司名称
    public String  enterprise_type_id;       //企业类型(( 1、施工单位 2、建设单位 3、运输单位 4、处置单位<消纳场处置单位、再生产品处置单位>)
    public String  address;           //公司地址
    public Double  enterprise_lng;
    public Double  enterprise_lat;
    public Integer department_id;     //当前用户部门id

    public String  province_id;
    public String  city_id;
    public String  area_id;
    public Integer dept_id;           //监管单位ID               ------------------------>参与所有关联

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public String  from_site_id;      //场地id
    public String  from_site_name;    //场地名称
    public String  from_site_type;    //工地类型(工地、回填)、处置类型
    public String  from_site_province_id;  //所在省
    public String  from_site_city_id;      //所在市
    public String  from_site_area_id;      //所在区
    public String  from_site_address; //地址
    public Double  from_site_lng;     //工地经度
    public Double  from_site_lat;     //工地纬度
    public Integer from_site_radius;  //半径(M)
    public String  from_site_range;   //区域范围
    public Integer from_sit_flag;     //1:工地 2:消纳场 3:黑工地 4:黑消纳场
    public String  from_site_inter_time;
    public String  from_site_work_time;    //最新工作时间
    public String  from_site_leave_time;   //离开时间

    public String  to_site_id;      //场地id
    public String  to_site_name;    //场地名称
    public String  to_site_type;    //工地类型(工地、回填)、处置类型
    public String  to_site_province_id;  //所在省
    public String  to_site_city_id;      //所在市
    public String  to_site_area_id;      //所在区
    public String  to_site_address; //地址
    public Double  to_site_lng;     //工地经度
    public Double  to_site_lat;     //工地纬度
    public Integer to_site_radius;  //半径(M)
    public String  to_site_range;   //区域范围
    public Integer to_sit_flag;     //1:工地 2:消纳场 3:黑工地 4:黑消纳场
    public String  to_site_inter_time;
    public String  to_site_work_time;    //最新工作时间
    public String  to_site_leave_time;   //离开时间
    public Integer  vehicle_empty = 1;   //1 空载 0满载


    public TangBean() {
    }

    public Integer getVehicle_id() {
        return vehicle_id;
    }

    public void setVehicle_id(Integer vehicle_id) {
        this.vehicle_id = vehicle_id;
    }

    public String getCar_card_number() {
        return car_card_number;
    }

    public void setCar_card_number(String car_card_number) {
        this.car_card_number = car_card_number;
    }

    public String getVehicle_model_id() {
        return vehicle_model_id;
    }

    public void setVehicle_model_id(String vehicle_model_id) {
        this.vehicle_model_id = vehicle_model_id;
    }

    public Float getApproved_tonnage() {
        return approved_tonnage;
    }

    public void setApproved_tonnage(Float approved_tonnage) {
        this.approved_tonnage = approved_tonnage;
    }

    public Double getLoad() {
        return load;
    }

    public void setLoad(Double load) {
        this.load = load;
    }

    public Integer getDriver_id() {
        return driver_id;
    }

    public void setDriver_id(Integer driver_id) {
        this.driver_id = driver_id;
    }

    public String getDriver_name() {
        return driver_name;
    }

    public void setDriver_name(String driver_name) {
        this.driver_name = driver_name;
    }

    public String getEnterprise_id() {
        return enterprise_id;
    }

    public void setEnterprise_id(String enterprise_id) {
        this.enterprise_id = enterprise_id;
    }

    public String getEnterprise_name() {
        return enterprise_name;
    }

    public void setEnterprise_name(String enterprise_name) {
        this.enterprise_name = enterprise_name;
    }

    public String getEnterprise_type_id() {
        return enterprise_type_id;
    }

    public void setEnterprise_type_id(String enterprise_type_id) {
        this.enterprise_type_id = enterprise_type_id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Double getEnterprise_lng() {
        return enterprise_lng;
    }

    public void setEnterprise_lng(Double enterprise_lng) {
        this.enterprise_lng = enterprise_lng;
    }

    public Double getEnterprise_lat() {
        return enterprise_lat;
    }

    public void setEnterprise_lat(Double enterprise_lat) {
        this.enterprise_lat = enterprise_lat;
    }

    public Integer getDepartment_id() {
        return department_id;
    }

    public void setDepartment_id(Integer department_id) {
        this.department_id = department_id;
    }

    public String getProvince_id() {
        return province_id;
    }

    public void setProvince_id(String province_id) {
        this.province_id = province_id;
    }

    public String getCity_id() {
        return city_id;
    }

    public void setCity_id(String city_id) {
        this.city_id = city_id;
    }

    public String getArea_id() {
        return area_id;
    }

    public void setArea_id(String area_id) {
        this.area_id = area_id;
    }

    public Integer getDept_id() {
        return dept_id;
    }

    public void setDept_id(Integer dept_id) {
        this.dept_id = dept_id;
    }

    public String getFrom_site_id() {
        return from_site_id;
    }

    public void setFrom_site_id(String from_site_id) {
        this.from_site_id = from_site_id;
    }

    public String getFrom_site_name() {
        return from_site_name;
    }

    public void setFrom_site_name(String from_site_name) {
        this.from_site_name = from_site_name;
    }

    public String getFrom_site_type() {
        return from_site_type;
    }

    public void setFrom_site_type(String from_site_type) {
        this.from_site_type = from_site_type;
    }

    public String getFrom_site_province_id() {
        return from_site_province_id;
    }

    public void setFrom_site_province_id(String from_site_province_id) {
        this.from_site_province_id = from_site_province_id;
    }

    public String getFrom_site_city_id() {
        return from_site_city_id;
    }

    public void setFrom_site_city_id(String from_site_city_id) {
        this.from_site_city_id = from_site_city_id;
    }

    public String getFrom_site_area_id() {
        return from_site_area_id;
    }

    public void setFrom_site_area_id(String from_site_area_id) {
        this.from_site_area_id = from_site_area_id;
    }

    public String getFrom_site_address() {
        return from_site_address;
    }

    public void setFrom_site_address(String from_site_address) {
        this.from_site_address = from_site_address;
    }

    public Double getFrom_site_lng() {
        return from_site_lng;
    }

    public void setFrom_site_lng(Double from_site_lng) {
        this.from_site_lng = from_site_lng;
    }

    public Double getFrom_site_lat() {
        return from_site_lat;
    }

    public void setFrom_site_lat(Double from_site_lat) {
        this.from_site_lat = from_site_lat;
    }

    public Integer getFrom_site_radius() {
        return from_site_radius;
    }

    public void setFrom_site_radius(Integer from_site_radius) {
        this.from_site_radius = from_site_radius;
    }

    public String getFrom_site_range() {
        return from_site_range;
    }

    public void setFrom_site_range(String from_site_range) {
        this.from_site_range = from_site_range;
    }

    public Integer getFrom_sit_flag() {
        return from_sit_flag;
    }

    public void setFrom_sit_flag(Integer from_sit_flag) {
        this.from_sit_flag = from_sit_flag;
    }

    public String getFrom_site_inter_time() {
        return from_site_inter_time;
    }

    public void setFrom_site_inter_time(String from_site_inter_time) {
        this.from_site_inter_time = from_site_inter_time;
    }

    public String getFrom_site_work_time() {
        return from_site_work_time;
    }

    public void setFrom_site_work_time(String from_site_work_time) {
        this.from_site_work_time = from_site_work_time;
    }

    public String getFrom_site_leave_time() {
        return from_site_leave_time;
    }

    public void setFrom_site_leave_time(String from_site_leave_time) {
        this.from_site_leave_time = from_site_leave_time;
    }

    public String getTo_site_id() {
        return to_site_id;
    }

    public void setTo_site_id(String to_site_id) {
        this.to_site_id = to_site_id;
    }

    public String getTo_site_name() {
        return to_site_name;
    }

    public void setTo_site_name(String to_site_name) {
        this.to_site_name = to_site_name;
    }

    public String getTo_site_type() {
        return to_site_type;
    }

    public void setTo_site_type(String to_site_type) {
        this.to_site_type = to_site_type;
    }

    public String getTo_site_province_id() {
        return to_site_province_id;
    }

    public void setTo_site_province_id(String to_site_province_id) {
        this.to_site_province_id = to_site_province_id;
    }

    public String getTo_site_city_id() {
        return to_site_city_id;
    }

    public void setTo_site_city_id(String to_site_city_id) {
        this.to_site_city_id = to_site_city_id;
    }

    public String getTo_site_area_id() {
        return to_site_area_id;
    }

    public void setTo_site_area_id(String to_site_area_id) {
        this.to_site_area_id = to_site_area_id;
    }

    public String getTo_site_address() {
        return to_site_address;
    }

    public void setTo_site_address(String to_site_address) {
        this.to_site_address = to_site_address;
    }

    public Double getTo_site_lng() {
        return to_site_lng;
    }

    public void setTo_site_lng(Double to_site_lng) {
        this.to_site_lng = to_site_lng;
    }

    public Double getTo_site_lat() {
        return to_site_lat;
    }

    public void setTo_site_lat(Double to_site_lat) {
        this.to_site_lat = to_site_lat;
    }

    public Integer getTo_site_radius() {
        return to_site_radius;
    }

    public void setTo_site_radius(Integer to_site_radius) {
        this.to_site_radius = to_site_radius;
    }

    public String getTo_site_range() {
        return to_site_range;
    }

    public void setTo_site_range(String to_site_range) {
        this.to_site_range = to_site_range;
    }

    public Integer getTo_sit_flag() {
        return to_sit_flag;
    }

    public void setTo_sit_flag(Integer to_sit_flag) {
        this.to_sit_flag = to_sit_flag;
    }

    public String getTo_site_inter_time() {
        return to_site_inter_time;
    }

    public void setTo_site_inter_time(String to_site_inter_time) {
        this.to_site_inter_time = to_site_inter_time;
    }

    public String getTo_site_work_time() {
        return to_site_work_time;
    }

    public void setTo_site_work_time(String to_site_work_time) {
        this.to_site_work_time = to_site_work_time;
    }

    public String getTo_site_leave_time() {
        return to_site_leave_time;
    }

    public void setTo_site_leave_time(String to_site_leave_time) {
        this.to_site_leave_time = to_site_leave_time;
    }

    public Integer getVehicle_empty() {
        return vehicle_empty;
    }

    public void setVehicle_empty(Integer vehicle_empty) {
        this.vehicle_empty = vehicle_empty;
    }

    @Override
    public String toString() {
        return "TangBean{" +
                "vehicle_id=" + vehicle_id +
                ", car_card_number='" + car_card_number + '\'' +
                ", vehicle_model_id='" + vehicle_model_id + '\'' +
                ", approved_tonnage=" + approved_tonnage +
                ", load=" + load +
                ", driver_id=" + driver_id +
                ", driver_name='" + driver_name + '\'' +
                ", enterprise_id='" + enterprise_id + '\'' +
                ", enterprise_name='" + enterprise_name + '\'' +
                ", enterprise_type_id='" + enterprise_type_id + '\'' +
                ", address='" + address + '\'' +
                ", enterprise_lng=" + enterprise_lng +
                ", enterprise_lat=" + enterprise_lat +
                ", department_id=" + department_id +
                ", province_id='" + province_id + '\'' +
                ", city_id='" + city_id + '\'' +
                ", area_id='" + area_id + '\'' +
                ", dept_id=" + dept_id +
                ", from_site_id='" + from_site_id + '\'' +
                ", from_site_name='" + from_site_name + '\'' +
                ", from_site_type='" + from_site_type + '\'' +
                ", from_site_province_id='" + from_site_province_id + '\'' +
                ", from_site_city_id='" + from_site_city_id + '\'' +
                ", from_site_area_id='" + from_site_area_id + '\'' +
                ", from_site_address='" + from_site_address + '\'' +
                ", from_site_lng=" + from_site_lng +
                ", from_site_lat=" + from_site_lat +
                ", from_site_radius=" + from_site_radius +
                ", from_site_range='" + from_site_range + '\'' +
                ", from_sit_flag=" + from_sit_flag +
                ", from_site_inter_time='" + from_site_inter_time + '\'' +
                ", from_site_work_time='" + from_site_work_time + '\'' +
                ", from_site_leave_time='" + from_site_leave_time + '\'' +
                ", to_site_id='" + to_site_id + '\'' +
                ", to_site_name='" + to_site_name + '\'' +
                ", to_site_type='" + to_site_type + '\'' +
                ", to_site_province_id='" + to_site_province_id + '\'' +
                ", to_site_city_id='" + to_site_city_id + '\'' +
                ", to_site_area_id='" + to_site_area_id + '\'' +
                ", to_site_address='" + to_site_address + '\'' +
                ", to_site_lng=" + to_site_lng +
                ", to_site_lat=" + to_site_lat +
                ", to_site_radius=" + to_site_radius +
                ", to_site_range='" + to_site_range + '\'' +
                ", to_sit_flag=" + to_sit_flag +
                ", to_site_inter_time='" + to_site_inter_time + '\'' +
                ", to_site_work_time='" + to_site_work_time + '\'' +
                ", to_site_leave_time='" + to_site_leave_time + '\'' +
                ", vehicle_empty=" + vehicle_empty +
                '}';
    }
}
