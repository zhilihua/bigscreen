package com.gkgd.bigscreen.entity.dwd;

import com.gkgd.bigscreen.entity.ods.TblPosinfo;

/**
 * @ModelName
 * @Description
 * @Author zhangjinhang
 * @Date 2020/10/27 17:43
 * @Version V1.0.0
 */
public class DataBusBean extends TblPosinfo {

    ///////////////////////////////////////////////////////车辆
    public Integer vehicle_id;       //车辆id                  ------------------------->关联服务费、双向登记卡、运输证
    public String  car_card_number;     //车牌号
    public String  vehicle_model_id;    //车辆类型 16 重型自卸货车 17中型自卸货车
    public String  vehicle_type_id;     //车辆种类（4综合执法车，5渣土车）
    public String  vehicle_type_state;   //车辆类型状态
    public String  vehicle_state;     //车辆状态(0：本地待命1：维修2：事故3：停用4：短盘5：审车6：营运7：异地待命8：停运9：禁运)
    public Integer if_new_energy;     //是否新能源（0否、1是、3.国三 4国四，5国五）
    public Float   approved_tonnage;      //核定吨位
    public Integer driver_id;         //司机ID                  ------------------------>关联司机表
    public String  enterprise_id;      //公司ID                  ------------------------>关联公司表
    public Integer dept_id;           //监管单位ID               ------------------------>参与所有关联
    public String audit_state;
    public String manage_state;
    ////////////////////////////////////////////////司机信息
    public String  driver_card_number;  //驾驶证
    public String  driver_name;         //司机姓名

    ////////////////////////////////////////////////公司信息
    public String  enterprise_name;    //公司名称
    public Integer department_id;     //当前用户部门id
    public String  address;           //公司地址
    public Double  enterprise_lng;
    public Double  enterprise_lat;
    public String  enterprise_type_id;       //企业类型(( 1、施工单位 2、建设单位 3、运输单位 4、处置单位<消纳场处置单位、再生产品处置单位>)
    public String  province_id;
    public String  city_id;
    public String  area_id;

    ///////////////////////////////////////////////双向登记卡信息：ods_cwp_vehicle_register_card
    public String  coords;                //路线  一辆车可以有多个双向登记卡，一张双向登记卡有多条路线：一张卡格式：[[[lng,lat],[lng,lat]],[[lng,lat],[lng,lat]]]  多张卡格式用|线分隔：[[[lng,lat],[lng,lat]],[[lng,lat],[lng,lat]]]|[[[lng,lat],[lng,lat]],[[lng,lat],[lng,lat]]]|.....
    public Integer register_card_state = 0;            //状态(1、正常 0、作废) --->对应表state
    ///////////////////////////////////////////////运输证信息：ods_cwp_vehicle_transport_card
    public Integer transport_card_state = 0;            //状态(0 已申请、1已批准、2已驳回、3作废)  --->对应表state

    /////////////////////////////////////////////// 服务到期信息:ods_cwp_vehicle_service_fee
    public Integer service_state;                     //'状态0 正常 1 过期'
    //////////////////////////////////////////////  定位区域:dim_cwp_area_coords
    public String region_id;                      //进入区域ID
    public String region_name;                    //进入区域名称
    //////////////////////////////////////////////

    public Integer at_work;                  //1:从工地出来的，有效满载车辆

    public Double mileage;


    public DataBusBean() {
        super();
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

    public String getVehicle_type_id() {
        return vehicle_type_id;
    }

    public void setVehicle_type_id(String vehicle_type_id) {
        this.vehicle_type_id = vehicle_type_id;
    }

    public String getVehicle_type_state() {
        return vehicle_type_state;
    }

    public void setVehicle_type_state(String vehicle_type_state) {
        this.vehicle_type_state = vehicle_type_state;
    }

    public String getVehicle_state() {
        return vehicle_state;
    }

    public void setVehicle_state(String vehicle_state) {
        this.vehicle_state = vehicle_state;
    }

    public Integer getIf_new_energy() {
        return if_new_energy;
    }

    public void setIf_new_energy(Integer if_new_energy) {
        this.if_new_energy = if_new_energy;
    }

    public Float getApproved_tonnage() {
        return approved_tonnage;
    }

    public void setApproved_tonnage(Float approved_tonnage) {
        this.approved_tonnage = approved_tonnage;
    }

    public Integer getDriver_id() {
        return driver_id;
    }

    public void setDriver_id(Integer driver_id) {
        this.driver_id = driver_id;
    }

    public String getEnterprise_id() {
        return enterprise_id;
    }

    public void setEnterprise_id(String enterprise_id) {
        this.enterprise_id = enterprise_id;
    }

    public Integer getDept_id() {
        return dept_id;
    }

    public void setDept_id(Integer dept_id) {
        this.dept_id = dept_id;
    }

    public String getDriver_card_number() {
        return driver_card_number;
    }

    public void setDriver_card_number(String driver_card_number) {
        this.driver_card_number = driver_card_number;
    }

    public String getDriver_name() {
        return driver_name;
    }

    public void setDriver_name(String driver_name) {
        this.driver_name = driver_name;
    }

    public String getEnterprise_name() {
        return enterprise_name;
    }

    public void setEnterprise_name(String enterprise_name) {
        this.enterprise_name = enterprise_name;
    }

    public Integer getDepartment_id() {
        return department_id;
    }

    public void setDepartment_id(Integer department_id) {
        this.department_id = department_id;
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

    public String getEnterprise_type_id() {
        return enterprise_type_id;
    }

    public void setEnterprise_type_id(String enterprise_type_id) {
        this.enterprise_type_id = enterprise_type_id;
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

    public String getCoords() {
        return coords;
    }

    public void setCoords(String coords) {
        this.coords = coords;
    }

    public Integer getRegister_card_state() {
        return register_card_state;
    }

    public void setRegister_card_state(Integer register_card_state) {
        this.register_card_state = register_card_state;
    }

    public Integer getTransport_card_state() {
        return transport_card_state;
    }

    public void setTransport_card_state(Integer transport_card_state) {
        this.transport_card_state = transport_card_state;
    }

    public Integer getService_state() {
        return service_state;
    }

    public void setService_state(Integer service_state) {
        this.service_state = service_state;
    }

    public String getRegion_id() {
        return region_id;
    }

    public void setRegion_id(String region_id) {
        this.region_id = region_id;
    }

    public String getRegion_name() {
        return region_name;
    }

    public void setRegion_name(String region_name) {
        this.region_name = region_name;
    }

    public Integer getAt_work() {
        return at_work;
    }

    public void setAt_work(Integer at_work) {
        this.at_work = at_work;
    }

    public String getAudit_state() {
        return audit_state;
    }

    public void setAudit_state(String audit_state) {
        this.audit_state = audit_state;
    }

    public String getManage_state() {
        return manage_state;
    }

    public void setManage_state(String manage_state) {
        this.manage_state = manage_state;
    }

    public Double getMileage() {
        return mileage;
    }

    public void setMileage(Double mileage) {
        this.mileage = mileage;
    }

    @Override
    public String toString() {
        return "DataBusBean{" +
                "devid='" + devid + '\'' +
                ", time='" + time + '\'' +
                ", lng=" + lng +
                ", lat=" + lat +
                ", point='" + point + '\'' +
                ", speed='" + speed + '\'' +
                ", pos='" + pos + '\'' +
                ", gpsflag='" + gpsflag + '\'' +
                ", wireflag='" + wireflag + '\'' +
                ", powerflag='" + powerflag + '\'' +
                ", accflag='" + accflag + '\'' +
                ", auxiliaryflag='" + auxiliaryflag + '\'' +
                ", t='" + t + '\'' +
                ", door='" + door + '\'' +
                ", alldistance='" + alldistance + '\'' +
                ", alldistfuel='" + alldistfuel + '\'' +
                ", gpsdistance='" + gpsdistance + '\'' +
                ", gpsalldistance='" + gpsalldistance + '\'' +
                ", createdatetime='" + createdatetime + '\'' +
                ", zxcqi='" + zxcqi + '\'' +
                ", alarmflag='" + alarmflag + '\'' +
                ", mainoilper='" + mainoilper + '\'' +
                ", residualoil='" + residualoil + '\'' +
                ", viceoilper='" + viceoilper + '\'' +
                ", viceresidualoil='" + viceresidualoil + '\'' +
                ", oildifference='" + oildifference + '\'' +
                ", sharpturn='" + sharpturn + '\'' +
                ", brake='" + brake + '\'' +
                ", vehicle_id=" + vehicle_id +
                ", car_card_number='" + car_card_number + '\'' +
                ", vehicle_model_id='" + vehicle_model_id + '\'' +
                ", vehicle_type_id='" + vehicle_type_id + '\'' +
                ", vehicle_type_state='" + vehicle_type_state + '\'' +
                ", vehicle_state='" + vehicle_state + '\'' +
                ", if_new_energy=" + if_new_energy +
                ", approved_tonnage=" + approved_tonnage +
                ", driver_id=" + driver_id +
                ", enterprise_id='" + enterprise_id + '\'' +
                ", dept_id=" + dept_id +
                ", audit_state='" + audit_state + '\'' +
                ", manage_state='" + manage_state + '\'' +
                ", driver_card_number='" + driver_card_number + '\'' +
                ", driver_name='" + driver_name + '\'' +
                ", enterprise_name='" + enterprise_name + '\'' +
                ", department_id=" + department_id +
                ", address='" + address + '\'' +
                ", enterprise_lng=" + enterprise_lng +
                ", enterprise_lat=" + enterprise_lat +
                ", enterprise_type_id='" + enterprise_type_id + '\'' +
                ", province_id='" + province_id + '\'' +
                ", city_id='" + city_id + '\'' +
                ", area_id='" + area_id + '\'' +
                ", coords='" + coords + '\'' +
                ", register_card_state=" + register_card_state +
                ", transport_card_state=" + transport_card_state +
                ", service_state=" + service_state +
                ", region_id='" + region_id + '\'' +
                ", region_name='" + region_name + '\'' +
                ", at_work=" + at_work +
                '}';
    }
}
