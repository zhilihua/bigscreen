package com.gkgd.bigscreen.jt8080;

public class ByteOperater {
//    private static final Logger logger = LoggerFactory.getLogger(ByteOperater.class);

    private BitOperater bitOperator;
    private BCD8421Operater bcd8421Operater;

    public ByteOperater() {
        this.bitOperator=new BitOperater();
        this.bcd8421Operater=new BCD8421Operater();
    }

    /**
     * 从byte数组中取出指定的数据转为十六进制,如果出错默认返回0
     * @param data
     * @param startIndex
     * @param length
     * @return
     */
    public int parseHexFromBytes(byte[] data, int startIndex, int length) {
        String hex=String.valueOf(parseIntFromBytes(data, startIndex, length, 0));
        // 十六进制转化为十进制，结果140。
        return Integer.parseInt(hex,16);
    }

    /**
     * 从byte数组中取出指定的数据转为int,如果出错默认返回0
     * @param data
     * @param startIndex
     * @param length
     * @return
     */
    public int parseIntFromBytes(byte[] data, int startIndex, int length) {
        return parseIntFromBytes(data, startIndex, length, 0);
    }

    private int parseIntFromBytes(byte[] data, int startIndex, int length, int defaultVal) {
        try {
            // 字节数大于4,从起始索引开始向后处理4个字节,其余超出部分丢弃
            final int len = length > 4 ? 4 : length;
            byte[] tmp = new byte[len];
            System.arraycopy(data, startIndex, tmp, 0, len);
            return bitOperator.byteToInteger(tmp);
        } catch (Exception e) {
//            logger.error("解析整数出错:{}", e.getMessage());
            e.printStackTrace();
            return defaultVal;
        }
    }

    public static byte[] arraycopy(byte[] data, int startIndex, int length) {
        byte[] tmp = new byte[length];
        System.arraycopy(data, startIndex, tmp, 0, length);
        return tmp;
    }


}
