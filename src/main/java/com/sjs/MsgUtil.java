package com.sjs;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

/**
 * @author ghe
 *         与C编写的服务器通信
 *         char 占1个字节
 */
public class MsgUtil {

    /*
     * Checksum
     * */
    public static int calChecksum(byte[] bufByte) {
        int length = bufByte.length;
        int sum = 0;
        if (length > 0) {
            for (int i = 0; i < length; i++) {
                sum += 0XFF & bufByte[i];
            }
        }
        return (sum % 256);
    }

    public static int calCheckSum(byte[] bufByte1, byte[] bufByte2) {
        int sum = 0;
        if (bufByte1 != null && bufByte1.length > 0) {
            for (int i = 0; i < bufByte1.length; i++) {
                sum += 0XFF & bufByte1[i];
            }
        }
        if (bufByte2 != null && bufByte2.length > 0) {
            for (int i = 0; i < bufByte2.length; i++) {
                sum += 0XFF & bufByte2[i];
            }
        }
        return (sum % 256);
    }

    public static boolean checksum(int checksum, byte[] bufByte) {
        return calChecksum(bufByte) == checksum;
    }

    public static boolean checksum(int checksum, byte[] bufByte1, byte[] bufByte2) {
        return calCheckSum(bufByte1, bufByte2) == checksum;
    }

    /**
     * @param bufByte
     * @return 转化为3位ASCII码，末尾追加空格.
     */
    public static byte[] getChecksum(byte[] bufByte) {
        int checksum = calChecksum(bufByte);
        return getChecksum(checksum);
    }

    public static byte[] getChecksum(int checksum) {
        return (alignRightWithZero(checksum + "", 3) + " ").getBytes();
    }


    /*
     *
     * String Utils
     * */
    public static String alignLeft(String str, int len) {
        if (str.length() == len) {
            return str;
        }
        StringBuffer sb = new StringBuffer(str);
        int blankSize = len - str.length();
        for (int i = 0; i < blankSize; i++) {
            sb.append(' ');
        }
        return sb.toString();
    }

    public static String alignRight(String str, int len) {
        if (str.length() == len) {
            return str;
        }
        StringBuffer sb = new StringBuffer();
        int blankSize = len - str.length();
        for (int i = 0; i < blankSize; i++) {
            sb.append(' ');
        }
        sb.append(str);
        return sb.toString();
    }

    public static String alignRightWithZero(String str, int len) {
        if (str.length() == len) {
            return str;
        }
        StringBuffer sb = new StringBuffer();
        int blankSize = len - str.length();
        for (int i = 0; i < blankSize; i++) {
            sb.append('0');
        }
        sb.append(str);
        return sb.toString();
    }


    /*
     * Byte Utils
     *
     * */
    public static byte[] bytesFromUint8(int i) {
        return toByteArray(i, 1);
    }

    // 大端模式
    public static byte[] bytesFromUint16(int i) {
        return toByteArray(i, 2);
    }

    // 大端模式
    public static byte[] bytesFromUint32(long l) {
        return toByteArray(l, 4);
    }

    public static byte[] toByteArray(int value, int arrayLen) {
        byte[] bytes = new byte[arrayLen];
        for (int i = 0; (i < 4) && (i < arrayLen); i++) {
            bytes[arrayLen - i - 1] = (byte) (value >> 8 * i & 0xff);
        }
        return bytes;
    }

    public static byte[] toByteArray(long value, int arrayLen) {
        byte[] bytes = new byte[arrayLen];
        for (int i = 0; (i < 4) && (i < arrayLen); i++) {
            bytes[arrayLen - i - 1] = (byte) (value >> 8 * i & 0xff);
        }
        return bytes;
    }

    /**
     * @param array  byte数组
     * @param offset 开始位
     * @return
     */
    public static int uint8FromBytes(byte[] array, int offset) {
        return (int) (array[offset] & 0xFF);
    }

    // 大端模式
    public static int uint16FromBytes(byte[] array, int offset) {
        return (int) ((array[offset + 1] & 0xFF)
                | ((array[offset] << 8) & 0xFF00));
    }

    // 小端模式
    // 已过时
    public static int uint16BizFromBytes1(byte[] array, int offset) {
        byte[] uint16 = new byte[3];
        uint16[2] = 0;
        System.arraycopy(array, offset, uint16, 0, 2);
        return new BigInteger(reverse(uint16)).intValue();
    }

    // 小端模式
    public static int uint16BizFromBytes(byte[] array, int offset) {
        return (int) ((array[offset] & 0xFF)
                | ((array[offset + 1] << 8) & 0xFF00));
    }

    // 大端模式，高字节在低地址位
    public static long uint32FromBytes(byte[] array, int offset) {
        return (long) ((array[offset + 3] & 0xFF)
                | ((array[offset + 2] << 8) & 0xFF00)
                | ((array[offset + 1] << 16) & 0xFF0000)
                | ((array[offset] << 24) & 0xFF000000));
    }

    // 小端模式，高字节在高地址位
    public static long uint32BizFromBytes(byte[] array, int offset) {
        return (long) ((array[offset] & 0xFF)
                | ((array[offset + 1] << 8) & 0xFF00)
                | ((array[offset + 2] << 16) & 0xFF0000)
                | ((array[offset + 3] << 24) & 0xFF000000));
    }


    private static byte[] reverse(byte[] bs) {
        for (int i = 0; i < bs.length / 2; i++) {
            byte temp = bs[i];
            bs[i] = bs[bs.length - 1 - i];
            bs[bs.length - 1 - i] = temp;
        }
        return bs;
    }

    // 小端模式
    // 已过时
    public static long int64FromBytes1(byte[] array, int offset) {
        byte[] int64 = new byte[8];
        System.arraycopy(array, offset, int64, 0, 8);
        return new BigInteger(reverse(int64)).longValue();
    }

    // 小端模式
    public static long int64FromBytes(byte[] array, int offset) {
        return (((long)array[offset] & 0xFFL)
                | (((long)array[offset + 1] << 8) & 0xFF00L)
                | (((long)array[offset + 2] << 16) & 0xFF0000L)
                | (((long)array[offset + 3] << 24) & 0xFF000000L)
                | (((long)array[offset + 4] << 32) & 0xFF00000000L)
                | (((long)array[offset + 5] << 40) & 0xFF0000000000L)
                | (((long)array[offset + 6] << 48) & 0xFF000000000000L)
                | (((long)array[offset + 7] << 56) & 0xFF00000000000000L));
    }


    // 小端模式
    // FIXME: java无uint64，8位是无法存储的，
    //  所以这个long内存是uint64，对于uint64使用时需要再转为BigInteger
    //  byte[] b = new byte[a.length + 1];
    //  b[a.length ] = 0;
    //  System.arraycopy(a, 0, b, 0, a.length);
    //  BigInteger bi1 = new BigInteger(reverse(b));
    //  System.out.println(bi1);
    public static long uint64FromBytes(byte[] array, int offset) {
        return (((long)array[offset] & 0xFFL)
                | (((long)array[offset + 1] << 8) & 0xFF00L)
                | (((long)array[offset + 2] << 16) & 0xFF0000L)
                | (((long)array[offset + 3] << 24) & 0xFF000000L)
                | (((long)array[offset + 4] << 32) & 0xFF00000000L)
                | (((long)array[offset + 5] << 40) & 0xFF0000000000L)
                | (((long)array[offset + 6] << 48) & 0xFF000000000000L)
                | (((long)array[offset + 7] << 56) & 0xFF00000000000000L));
    }



    public static String stringFromBytes(byte[] array, int offset, int length, String encoding) throws UnsupportedEncodingException {
        byte[] strBytes = new byte[length];
        for (int i = 0; i < length; i++) {
            strBytes[i] = array[offset + i];
        }
        return new String(strBytes, encoding);
    }

    public static byte[] bytesFromBytes(byte[] array, int offset, int length) throws UnsupportedEncodingException {
        byte[] strBytes = new byte[length];
        for (int i = 0; i < length; i++) {
            strBytes[i] = array[offset + i];
        }
        return strBytes;
    }

    public static byte byteFromBytes(byte[] array, int offset) throws UnsupportedEncodingException {
        return array[offset];
    }
	
	
	/*Date Utils*/

    public static String formatDayIn8Chars(Date date) {
        return DateFormatUtils.format(date, "yyyyMMdd");
    }

    public static String formatFullDateTime(Date date) {
        return DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss.SSS");
    }

    public static String formatDayIn10Chars(Date date) {
        return DateFormatUtils.format(date, "yyyy-MM-dd");
    }

    public static long getTodayMillis(String HMS) {
        String timeStr = formatDayIn10Chars(new Date()) + " " + HMS;
        return Timestamp.valueOf(timeStr).getTime();
    }

    public static Date parseLongCharts(long longChar) {
        try {
            return DateUtils.parseDate(longChar + "", new String[]{"yyyyMMddHHmmssSSS"});
        } catch (Exception e) {
            return null;
        }
    }
	
	
	/*Number Utils*/
    public static int parseInt(final String s, final int defaultValue) {
        return s==null||s.length()==0 ? defaultValue : Integer.parseInt(s);
    }

    public static int parseInt(final String s) {
        return parseInt(s, 0);
    }

    public static String byteToHex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        String hex;
        for (int i = 0; i < b.length; ++i) {
            hex = Integer.toHexString(b[i] & 0xFF);
            if (hex.length() == 1) {
                hex = '0' + hex;
                sb.append(hex);
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println(uint32FromBytes(bytesFromUint32(2039224L), 0));

    }

}
