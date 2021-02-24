package com.gkgd.bigscreen.jt8080;

public class BCD8421Operater {
//    private static final Logger logger = LoggerFactory.getLogger(BCD8421Operater.class);

    /**
     *
     * BCD字节数组===>String
     *
     * @param bytes
     * @return 十进制字符串
     */
    public String bcd2String(byte[] bytes) {
        StringBuilder temp = new StringBuilder(bytes.length * 2);
        for (int i = 0; i < bytes.length; i++) {
            // 高四位
            temp.append((bytes[i] & 0xf0) >>> 4);
            // 低四位
            temp.append(bytes[i] & 0x0f);
        }
        return temp.toString().substring(0, 1).equalsIgnoreCase("0") ? temp.toString().substring(1) : temp.toString();
    }

    /**
     * 字符串==>BCD字节数组
     *
     * @param str
     * @return BCD字节数组
     */
    public byte[] string2Bcd(String str) {
        // 奇数,前补零
        if ((str.length() & 0x1) == 1) {
            str = "0" + str;
        }

        byte ret[] = new byte[str.length() / 2];
        byte bs[] = str.getBytes();
        for (int i = 0; i < ret.length; i++) {

            byte high = ascII2Bcd(bs[2 * i]);
            byte low = ascII2Bcd(bs[2 * i + 1]);

            // TODO 只遮罩BCD低四位?
            ret[i] = (byte) ((high << 4) | low);
        }
        return ret;
    }

    private byte ascII2Bcd(byte asc) {
        if ((asc >= '0') && (asc <= '9'))
            return (byte) (asc - '0');
        else if ((asc >= 'A') && (asc <= 'F'))
            return (byte) (asc - 'A' + 10);
        else if ((asc >= 'a') && (asc <= 'f'))
            return (byte) (asc - 'a' + 10);
        else
            return (byte) (asc - 48);
    }

    /**
     * 从byte数组中取出指定的数据转为BCD,如果出错默认返回null
     *
     * @param data
     * @param startIndex
     * @param lenth
     * @return
     */
    public String parseBcdStringFromBytes(byte[] data, int startIndex, int lenth) {
        return parseBcdStringFromBytes(data, startIndex, lenth, null);
    }

    private String parseBcdStringFromBytes(byte[] data, int startIndex, int lenth, String defaultVal) {
        try {
            byte[] tmp = new byte[lenth];
            System.arraycopy(data, startIndex, tmp, 0, lenth);
            return bcd2String(tmp);
        } catch (Exception e) {
//            logger.error("解析BCD(8421码)出错:{}", e);
            return defaultVal;
        }
    }
}
