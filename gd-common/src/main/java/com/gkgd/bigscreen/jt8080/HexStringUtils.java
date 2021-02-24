package com.gkgd.bigscreen.jt8080;

import java.io.Serializable;

public class HexStringUtils implements Serializable {

    private static final char[] DIGITS_HEX = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    protected static char[] encodeHex(byte[] data) {
        int l = data.length;
        char[] out = new char[l << 1];
        for (int i = 0, j = 0; i < l; i++) {
            out[j++] = DIGITS_HEX[(0xF0 & data[i]) >>> 4];
            out[j++] = DIGITS_HEX[0x0F & data[i]];
        }
        return out;
    }

    protected static byte[] decodeHex(char[] data) {
        int len = data.length;
        if ((len & 0x01) != 0) {
            throw new RuntimeException("字符个数应该为偶数");
        }
        byte[] out = new byte[len >> 1];
        for (int i = 0, j = 0; j < len; i++) {
            int f = toDigit(data[j], j) << 4;
            j++;
            f |= toDigit(data[j], j);
            j++;
            out[i] = (byte) (f & 0xFF);
        }
        return out;
    }

    protected static int toDigit(char ch, int index) {
        int digit = Character.digit(ch, 16);
        if (digit == -1) {
            throw new RuntimeException("Illegal hexadecimal character " + ch + " at index " + index);
        }
        return digit;
    }

    public static String toHexString(byte[] bs, int startIndex, int length) {
        try{
            byte[] tmp = new byte[length];
            System.arraycopy(bs, startIndex, tmp, 0, length);
            return toHexString(tmp);
        }catch (Exception e){
            return "";
        }
    }

    public static String toHexString(byte[] bs) {
        return new String(encodeHex(bs));
    }

    public static String hexString2Bytes(String hex) {
        return new String(decodeHex(hex.toCharArray()));
    }

    public static byte[] chars2Bytes(char[] bs) {
        return decodeHex(bs);
    }

    /**
     * 十进制转化为十六进制
     *
     * @param number
     * @return
     */
    public static String toHex(int number) {
        return Integer.toHexString(number);    // 十进制转化为十六进制，结果为C8
    }

    /**
     * @param hexNumber 需要转的十六进制数
     * @param n         需要格式化的位数
     * @return 二进制字符串
     */
    public static String toBinaryStringByHexNumber(int hexNumber, int n) {
        //先转为字符串
        String hexNumberStr = String.valueOf(hexNumber);
        //解析为十进制
        int number = Integer.parseInt(hexNumberStr, 16);
        return toBinaryStringByNumber(number, n);
    }

    /**
     * @param number 需要转的十进制数
     * @param n      需要格式化的位数
     * @return 二进制字符串
     */
    public static String toBinaryStringByNumber(long number, int n) {
        //再转为二进制并格式化
        String binaryStr = Long.toBinaryString(number);
        return String.format("%0" + n + "d", Long.parseLong(binaryStr)); //0 代表前面补充0  d 代表参数为正数型
    }

    /**
     * hex字符串 转为 byte[]
     *
     * @param hexString the hex string
     * @return byte[]
     */
    public static byte[] hexStringToBytes(String hexString) {
        if (hexString == null || hexString.equals("")) {
            return null;
        }
        hexString = hexString.toUpperCase();
        int length = hexString.length() / 2;
        char[] hexChars = hexString.toCharArray();
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return d;
    }

    /**
     * Convert char to byte
     *
     * @param c char
     * @return byte
     */
    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }

}
