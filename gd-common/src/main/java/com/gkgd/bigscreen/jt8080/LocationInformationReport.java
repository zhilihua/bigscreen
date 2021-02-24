package com.gkgd.bigscreen.jt8080;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 位置信息上报
 *
 * @author Administrator
 */
public class LocationInformationReport implements Serializable {
    /**
     * 报警标识
     **/
    private String alarm;
    private AlarmInfo alarmInfo;
    /**
     * 状态标识
     **/
    private String state;
    private StateInfo stateInfo;
    /**
     * 纬度
     **/
    private String latitude;
    /**
     * 经度
     **/
    private String longitude;
    /**
     * 高程 海拔高度，单位为米(m)
     **/
    private String altitude;
    /**
     * 速度
     **/
    private String speed;
    /**
     * 方向
     **/
    private String direction;
    /**
     * 时间
     **/
    private String dateTime;
    /**
     * 附加信息
     **/
    private List<AdditionalInformation> additionalInformationList;
    private Map<Integer, AdditionalInformation> additionalInformationMap;

    private static BitOperater bitOperater = new BitOperater();

    /**
     * 根据data
     *
     * @param data
     * @return
     */
    public static LocationInformationReport getEntity(byte[] data) {
        HexStringUtils hexStringUtils = new HexStringUtils();
        BCD8421Operater bcd8421Operater = new BCD8421Operater();
        LocationInformationReport locationInformationReport = new LocationInformationReport();
        locationInformationReport.alarm = hexStringUtils.toHexString(data, 0, 4);
        locationInformationReport.setAlarmInfo(new AlarmInfo(locationInformationReport.alarm));
        locationInformationReport.state = hexStringUtils.toHexString(data, 4, 4);
        locationInformationReport.setStateInfo(new StateInfo(locationInformationReport.state));
        locationInformationReport.latitude = hexStringUtils.toHexString(data, 8, 4);
        locationInformationReport.longitude = hexStringUtils.toHexString(data, 12, 4);
        locationInformationReport.altitude = hexStringUtils.toHexString(data, 16, 2);
        locationInformationReport.speed = hexStringUtils.toHexString(data, 18, 2);
        locationInformationReport.direction = hexStringUtils.toHexString(data, 20, 2);
        locationInformationReport.dateTime = bcd8421Operater.parseBcdStringFromBytes(data, 22, 6);
        locationInformationReport.additionalInformationList = new ArrayList<AdditionalInformation>();
        getAdditionalInformationList(locationInformationReport.additionalInformationList, data, 28);
        locationInformationReport.additionalInformationMap = new HashMap<>();
        for (AdditionalInformation additionalInformation : locationInformationReport.additionalInformationList) {
            locationInformationReport.additionalInformationMap.put(additionalInformation.getId(), additionalInformation);
        }
        return locationInformationReport;
    }

    private static void getAdditionalInformationList(List<AdditionalInformation> result, byte[] data, int srcPos) {
        try{
            int id = bitOperater.oneByteToInteger(data[srcPos]);
            int length = data[srcPos + 1];
            byte[] information = new byte[length];
            System.arraycopy(data, srcPos + 2, information, 0, length);
            result.add(new AdditionalInformation(id, length, information));
            if (data.length > (srcPos + 2 + length)) {
                getAdditionalInformationList(result, data, srcPos + 2 + length);
            }
        }catch (Exception e){
//            System.out.println(e.getMessage());
        }

    }

    public AlarmInfo getAlarmInfo() {
        return alarmInfo;
    }

    public void setAlarmInfo(AlarmInfo alarmInfo) {
        this.alarmInfo = alarmInfo;
    }

    public String getAlarm() {
        return alarm;
    }

    public void setAlarm(String alarm) {
        this.alarm = alarm;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getAltitude() {
        return altitude;
    }

    public void setAltitude(String altitude) {
        this.altitude = altitude;
    }

    public String getSpeed() {
        return speed;
    }

    public void setSpeed(String speed) {
        this.speed = speed;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public StateInfo getStateInfo() {
        return stateInfo;
    }

    public void setStateInfo(StateInfo stateInfo) {
        this.stateInfo = stateInfo;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public List<AdditionalInformation> getAdditionalInformationList() {
        return additionalInformationList;
    }

    public void setAdditionalInformationList(List<AdditionalInformation> additionalInformationList) {
        this.additionalInformationList = additionalInformationList;
    }

    public Map<Integer, AdditionalInformation> getAdditionalInformationMap() {
        return additionalInformationMap;
    }

    public void setAdditionalInformationMap(Map<Integer, AdditionalInformation> additionalInformationMap) {
        this.additionalInformationMap = additionalInformationMap;
    }

    public static class StateInfo {
        private String hexString;
        private String binaryString;
        /**
         * 0: ACC关;1:ACC开
         **/
        private int acc;
        /**
         * 0:未定位;1:定位
         **/
        private int location;
        /**
         * 0:北纬:1:南纬
         **/
        private int isNorthOrSouth;
        /**
         * 0:东经;1:西经
         **/
        private int isEastOrWest;
        /**
         * 0:运营状态:1:停运状态
         **/
        private int operate;
        /**
         * 0:经纬度未经保密插件加密;l:经纬度已经保密插件加密
         **/
        private int isEncryption;
        /**
         * 0:车辆油路正常:1:车辆油路断开
         **/
        private int oil;
        /**
         * 0:车辆电路正常:1:车辆电路断开
         **/
        private int circuit;
        /**
         * 0:车门解锁；1：车门加锁
         **/
        private int doorLock;

        public StateInfo(String hexString) {
            this.hexString = hexString;
            this.binaryString = BitOperater.hexStr2BinStr(hexString);
            Long stateMark = Long.parseLong(hexString, 16);
            BitOperater bitOperator = new BitOperater();
            this.acc = bitOperator.getBitAt(stateMark, 0);
            this.location = bitOperator.getBitAt(stateMark, 1);
            this.isNorthOrSouth = bitOperator.getBitAt(stateMark, 2);
            this.isEastOrWest = bitOperator.getBitAt(stateMark, 3);
            this.operate = bitOperator.getBitAt(stateMark, 4);
            this.isEncryption = bitOperator.getBitAt(stateMark, 5);
            this.oil = bitOperator.getBitAt(stateMark, 10);
            this.circuit = bitOperator.getBitAt(stateMark, 11);
            this.doorLock = bitOperator.getBitAt(stateMark, 12);
        }

        public int getAcc() {
            return acc;
        }

        public void setAcc(int acc) {
            this.acc = acc;
        }

        public int getLocation() {
            return location;
        }

        public void setLocation(int location) {
            this.location = location;
        }

        public int getIsNorthOrSouth() {
            return isNorthOrSouth;
        }

        public void setIsNorthOrSouth(int isNorthOrSouth) {
            this.isNorthOrSouth = isNorthOrSouth;
        }

        public int getIsEastOrWest() {
            return isEastOrWest;
        }

        public void setIsEastOrWest(int isEastOrWest) {
            this.isEastOrWest = isEastOrWest;
        }

        public int getOperate() {
            return operate;
        }

        public void setOperate(int operate) {
            this.operate = operate;
        }

        public int getIsEncryption() {
            return isEncryption;
        }

        public void setIsEncryption(int isEncryption) {
            this.isEncryption = isEncryption;
        }

        public int getOil() {
            return oil;
        }

        public void setOil(int oil) {
            this.oil = oil;
        }

        public int getCircuit() {
            return circuit;
        }

        public void setCircuit(int circuit) {
            this.circuit = circuit;
        }

        public int getDoorLock() {
            return doorLock;
        }

        public void setDoorLock(int doorLock) {
            this.doorLock = doorLock;
        }

        public String getHexString() {
            return hexString;
        }

        public void setHexString(String hexString) {
            this.hexString = hexString;
        }

        public String getBinaryString() {
            return binaryString;
        }

        public void setBinaryString(String binaryString) {
            this.binaryString = binaryString;
        }
    }

    public static class AlarmInfo {
        private String hexString;
        private String binaryString;
        /**
         * 1:紧急报警触动报警开关后触发
         **/
        private int touchAlarm;
        /**
         * 1：超速报警
         **/
        private int speed;
        /**
         * 1：疲劳驾驶
         **/
        private int fatigueDriving;
        /**
         * 1：预警
         **/
        private int earlyWarning;
        /**
         * 1：GNSS模块发生故障
         **/
        private int GNSS;
        /**
         * 1：GNSS天线未接或被剪断
         **/
        private int GNSSAntenna;
        /**
         * 1：GNSS天线短路
         **/
        private int GNSSShortCircuit;
        /**
         * 1：终端主电源欠压
         **/
        private int undervoltage;
        /**
         * 1：终端主电源掉电
         **/
        private int powerDown;
        /**
         * 1：终端LCD或显示器故障
         **/
        private int LCD;
        /**
         * 1：TTS模块故障
         **/
        private int TTS;
        /**
         * 1:摄像头故障
         **/
        private int camera;
        /**
         * 1:当天累计驾驶超时
         **/
        private int drivingTimeout;
        /**
         * 1：超时停车
         **/
        private int overtimeParking;
        /**
         * 1：进出区域
         **/
        private int entryAndExitAreas;
        /**
         * 1:进出路线
         **/
        private int entryAndExitRoutes;
        /**
         * 1:路段行驶时间不足/过长
         **/
        private int travelTime;
        /**
         * 1:路线偏离报警
         **/
        private int routeDeviation;
        /**
         * 1：车辆VSS故障
         **/
        private int VSS;
        /**
         * 1：车辆油量异常
         **/
        private int oil;
        /**
         * 1：车辆被盗(通过车辆防盗器)
         **/
        private int stolenVehicles;
        /**
         * 1：车辆非法点火
         **/
        private int illegalIgnition;
        /**
         * 1：车辆非法位移
         **/
        private int illegalDisplacement;
        /**
         * 急刹车
         **/
        private int brake;
        /**
         * 急转弯
         **/
        private int sharpturn;

        public AlarmInfo(String hexString) {
            this.hexString = hexString;
            this.binaryString = BitOperater.hexStr2BinStr(hexString);
            Long alarmMark = Long.parseLong(hexString, 16);
            BitOperater bitOperator = new BitOperater();
            this.touchAlarm = bitOperator.getBitAt(alarmMark, 0);
            this.speed = bitOperator.getBitAt(alarmMark, 1);
            this.fatigueDriving = bitOperator.getBitAt(alarmMark, 2);
            this.earlyWarning = bitOperator.getBitAt(alarmMark, 3);
            this.GNSS = bitOperator.getBitAt(alarmMark, 4);
            this.GNSSAntenna = bitOperator.getBitAt(alarmMark, 5);
            this.GNSSShortCircuit = bitOperator.getBitAt(alarmMark, 6);
            this.undervoltage = bitOperator.getBitAt(alarmMark, 7);
            this.powerDown = bitOperator.getBitAt(alarmMark, 8);
            this.LCD = bitOperator.getBitAt(alarmMark, 9);
            this.TTS = bitOperator.getBitAt(alarmMark, 10);
            this.camera = bitOperator.getBitAt(alarmMark, 11);
            this.sharpturn = bitOperator.getBitAt(alarmMark, 15);//急转弯
            this.brake = bitOperator.getBitAt(alarmMark, 17);//急刹车
            this.drivingTimeout = bitOperator.getBitAt(alarmMark, 18);
            this.overtimeParking = bitOperator.getBitAt(alarmMark, 19);
            this.entryAndExitAreas = bitOperator.getBitAt(alarmMark, 20);
            this.entryAndExitRoutes = bitOperator.getBitAt(alarmMark, 21);
            this.travelTime = bitOperator.getBitAt(alarmMark, 22);
            this.routeDeviation = bitOperator.getBitAt(alarmMark, 23);
            this.VSS = bitOperator.getBitAt(alarmMark, 24);
            this.oil = bitOperator.getBitAt(alarmMark, 25);
            this.stolenVehicles = bitOperator.getBitAt(alarmMark, 26);
            this.illegalIgnition = bitOperator.getBitAt(alarmMark, 28);
            this.illegalDisplacement = bitOperator.getBitAt(alarmMark, 29);
        }

        public String getHexString() {
            return hexString;
        }

        public void setHexString(String hexString) {
            this.hexString = hexString;
        }

        public int getTouchAlarm() {
            return touchAlarm;
        }

        public void setTouchAlarm(int touchAlarm) {
            this.touchAlarm = touchAlarm;
        }

        public int getSpeed() {
            return speed;
        }

        public void setSpeed(int speed) {
            this.speed = speed;
        }

        public int getFatigueDriving() {
            return fatigueDriving;
        }

        public void setFatigueDriving(int fatigueDriving) {
            this.fatigueDriving = fatigueDriving;
        }

        public int getEarlyWarning() {
            return earlyWarning;
        }

        public void setEarlyWarning(int earlyWarning) {
            this.earlyWarning = earlyWarning;
        }

        public int getGNSS() {
            return GNSS;
        }

        public void setGNSS(int gNSS) {
            GNSS = gNSS;
        }

        public int getGNSSAntenna() {
            return GNSSAntenna;
        }

        public void setGNSSAntenna(int gNSSAntenna) {
            GNSSAntenna = gNSSAntenna;
        }

        public int getGNSSShortCircuit() {
            return GNSSShortCircuit;
        }

        public void setGNSSShortCircuit(int gNSSShortCircuit) {
            GNSSShortCircuit = gNSSShortCircuit;
        }

        public int getUndervoltage() {
            return undervoltage;
        }

        public void setUndervoltage(int undervoltage) {
            this.undervoltage = undervoltage;
        }

        public int getPowerDown() {
            return powerDown;
        }

        public void setPowerDown(int powerDown) {
            this.powerDown = powerDown;
        }

        public int getLCD() {
            return LCD;
        }

        public void setLCD(int lCD) {
            LCD = lCD;
        }

        public int getTTS() {
            return TTS;
        }

        public void setTTS(int tTS) {
            TTS = tTS;
        }

        public int getCamera() {
            return camera;
        }

        public void setCamera(int camera) {
            this.camera = camera;
        }

        public int getDrivingTimeout() {
            return drivingTimeout;
        }

        public void setDrivingTimeout(int drivingTimeout) {
            this.drivingTimeout = drivingTimeout;
        }

        public int getOvertimeParking() {
            return overtimeParking;
        }

        public void setOvertimeParking(int overtimeParking) {
            this.overtimeParking = overtimeParking;
        }

        public int getEntryAndExitAreas() {
            return entryAndExitAreas;
        }

        public void setEntryAndExitAreas(int entryAndExitAreas) {
            this.entryAndExitAreas = entryAndExitAreas;
        }

        public int getEntryAndExitRoutes() {
            return entryAndExitRoutes;
        }

        public void setEntryAndExitRoutes(int entryAndExitRoutes) {
            this.entryAndExitRoutes = entryAndExitRoutes;
        }

        public int getTravelTime() {
            return travelTime;
        }

        public void setTravelTime(int travelTime) {
            this.travelTime = travelTime;
        }

        public int getRouteDeviation() {
            return routeDeviation;
        }

        public void setRouteDeviation(int routeDeviation) {
            this.routeDeviation = routeDeviation;
        }

        public int getVSS() {
            return VSS;
        }

        public void setVSS(int vSS) {
            VSS = vSS;
        }

        public int getOil() {
            return oil;
        }

        public void setOil(int oil) {
            this.oil = oil;
        }

        public int getStolenVehicles() {
            return stolenVehicles;
        }

        public void setStolenVehicles(int stolenVehicles) {
            this.stolenVehicles = stolenVehicles;
        }

        public int getIllegalIgnition() {
            return illegalIgnition;
        }

        public void setIllegalIgnition(int illegalIgnition) {
            this.illegalIgnition = illegalIgnition;
        }

        public int getIllegalDisplacement() {
            return illegalDisplacement;
        }

        public void setIllegalDisplacement(int illegalDisplacement) {
            this.illegalDisplacement = illegalDisplacement;
        }

        public int getBrake() {
            return brake;
        }

        public void setBrake(int brake) {
            this.brake = brake;
        }

        public int getSharpturn() {
            return sharpturn;
        }

        public void setSharpturn(int sharpturn) {
            this.sharpturn = sharpturn;
        }

        public String getBinaryString() {
            return binaryString;
        }

        public void setBinaryString(String binaryString) {
            this.binaryString = binaryString;
        }
    }

    public static class AdditionalInformation {
        private String hexString;
        private int id;
        private int length;
        private byte[] information;

        public AdditionalInformation(int id, int length, byte[] information) {
            this.id = id;
            this.length = length;
            this.information = information;
            StringBuffer sb = new StringBuffer();
            sb.append(HexStringUtils.toBinaryStringByNumber(id, 8))
                    .append(HexStringUtils.toBinaryStringByNumber(length, 8));
            sb.append(BitOperater.bytes2BinStr(information));
//			for(int i=0;i<length;i++){
//				sb.append(HexStringUtils.toBinaryStringByHexNumber(information[i],8));
//			}
            this.hexString = sb.toString();
        }

        public String getHexString() {
            return hexString;
        }

        public void setHexString(String hexString) {
            this.hexString = hexString;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }

        public byte[] getInformation() {
            return information;
        }

        public void setInformation(byte[] information) {
            this.information = information;
        }

    }
}
