package com.gkgd.bigscreen.jt8080;

public class DataDecode {

    public static void decode(String hexString){
        byte[] input = HexStringUtils.chars2Bytes(hexString.toCharArray());
        PackageData packageData = new MsgDecoderUtil().bytes2PackageData(input);
        PackageData.MsgHeader header = packageData.getMsgHeader();
        int msgId = header.getMsgId();

    }


}
