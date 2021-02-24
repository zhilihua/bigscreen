package com.gkgd.bigscreen.jt8080;

import java.io.Serializable;

public class MsgDecoderUtil implements Serializable {
//    private final static Logger logger = LoggerFactory.getLogger(MsgDecoderUtil.class);

    private BitOperater bitOperater;
    private BCD8421Operater bcd8421Operater;
    private ByteOperater byteOperater;

    public MsgDecoderUtil() {
        this.bitOperater = new BitOperater();
        this.bcd8421Operater = new BCD8421Operater();
        this.byteOperater = new ByteOperater();
    }

    /**
     * 将byte封装成PackageData
     *
     * @param data
     * @return
     */
    public PackageData bytes2PackageData(byte[] data) {
        PackageData ret = new PackageData();

        // 0. 终端套接字地址信息
//		ret.setSession(Session.buildSession(session));

        // 1. 16byte 或 12byte 消息头
        PackageData.MsgHeader msgHeader = this.parseMsgHeaderFromBytes(data);
        ret.setMsgHeader(msgHeader);

        int msgBodyByteStartIndex = 12;
        // 2. 消息体
        // 有子包信息,消息体起始字节后移四个字节:消息包总数(word(16))+包序号(word(16))
        if (msgHeader.isHasSubPackage()) {
            msgBodyByteStartIndex = 16;
        }

        byte[] tmp = new byte[msgHeader.getMsgBodyLength()];
        System.arraycopy(data, msgBodyByteStartIndex, tmp, 0, tmp.length);
        ret.setMsgBodyBytes(tmp);

        // 3. 去掉分隔符之后，最后一位就是校验码
        // int checkSumInPkg =
        // this.bitOperator.oneByteToInteger(data[data.length - 1]);
        int checkSumInPkg = data[data.length - 1];
        int calculatedCheckSum = bitOperater.getCheckSum4JT808(data, 0, data.length - 1);
        ret.setCheckSum(checkSumInPkg);
//        if (checkSumInPkg != calculatedCheckSum) {
//            logger.warn("检验码不一致,msgid:{},pkg:{},calculated:{}", msgHeader.getMsgId(), checkSumInPkg, calculatedCheckSum);
//        }
        return ret;
    }

    /**
     * 解析消息头
     *
     * @param data
     * @return
     */
    private PackageData.MsgHeader parseMsgHeaderFromBytes(byte[] data) {
        PackageData.MsgHeader msgHeader = new PackageData.MsgHeader();

        // 1. 消息ID word(16)
        // byte[] tmp = new byte[2];
        // System.arraycopy(data, 0, tmp, 0, 2);
        // msgHeader.setMsgId(this.bitOperator.twoBytesToInteger(tmp));
        msgHeader.setMsgId(byteOperater.parseIntFromBytes(data, 0, 2));

        // 2. 消息体属性 word(16)=================>
        // System.arraycopy(data, 2, tmp, 0, 2);
        // int msgBodyProps = this.bitOperator.twoBytesToInteger(tmp);
        int msgBodyProps = byteOperater.parseIntFromBytes(data, 2, 2);
        msgHeader.setMsgBodyPropsField(msgBodyProps);
        // [ 0-9 ] 0000,0011,1111,1111(3FF)(消息体长度)
        msgHeader.setMsgBodyLength(msgBodyProps & 0x3ff);
        // [10-12] 0001,1100,0000,0000(1C00)(加密类型)
        msgHeader.setEncryptionType((msgBodyProps & 0x1c00) >> 10);
        // [ 13_ ] 0010,0000,0000,0000(2000)(是否有子包)
        msgHeader.setHasSubPackage(((msgBodyProps & 0x2000) >> 13) == 1);
        // [14-15] 1100,0000,0000,0000(C000)(保留位)
        msgHeader.setReservedBit(((msgBodyProps & 0xc000) >> 14) + "");
        // 消息体属性 word(16)<=================

        // 3. 终端手机号 bcd[6]
        // tmp = new byte[6];
        // System.arraycopy(data, 4, tmp, 0, 6);
        // msgHeader.setTerminalPhone(this.bcd8421Operater.bcd2String(tmp));
        msgHeader.setTerminalPhone(bcd8421Operater.parseBcdStringFromBytes(data, 4, 6));

        // 4. 消息流水号 word(16) 按发送顺序从 0 开始循环累加
        // tmp = new byte[2];
        // System.arraycopy(data, 10, tmp, 0, 2);
        // msgHeader.setFlowId(this.bitOperator.twoBytesToInteger(tmp));
        msgHeader.setFlowId(byteOperater.parseIntFromBytes(data, 10, 2));

        // 5. 消息包封装项
        // 有子包信息
        if (msgHeader.isHasSubPackage()) {
            // 消息包封装项字段
            msgHeader.setPackageInfoField(byteOperater.parseIntFromBytes(data, 12, 4));
            // byte[0-1] 消息包总数(word(16))
            // tmp = new byte[2];
            // System.arraycopy(data, 12, tmp, 0, 2);
            // msgHeader.setTotalSubPackage(this.bitOperator.twoBytesToInteger(tmp));
            msgHeader.setTotalSubPackage(byteOperater.parseIntFromBytes(data, 12, 2));

            // byte[2-3] 包序号(word(16)) 从 1 开始
            // tmp = new byte[2];
            // System.arraycopy(data, 14, tmp, 0, 2);
            // msgHeader.setSubPackageSeq(this.bitOperator.twoBytesToInteger(tmp));
            msgHeader.setSubPackageSeq(byteOperater.parseIntFromBytes(data, 12, 2));
        }
        return msgHeader;
    }

}
