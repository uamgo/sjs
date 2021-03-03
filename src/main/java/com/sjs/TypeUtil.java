package com.sjs;

/**
 * Created by fengye on 2019/3/7.
 */
public class TypeUtil {

    final static int month_offset[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

    /**
     * 特殊的日期转换函数：long：2019030110111123 转 timestamp
     * @param pNumDatTim
     * @return
     */
    public static long NumTim2BinTim(long pNumDatTim) {
        long passedDay;
        long retTime;
        int year, month, day, hour, minute, second, milsecond;

        //get year, month, day, hour, minute, second, milsecond
        if (pNumDatTim > 1000000000000000L & pNumDatTim < 9999999999999999L) {
            milsecond = (int) (pNumDatTim % 100L);
            pNumDatTim = pNumDatTim / 100L;
            second = (int) (pNumDatTim % 100L);
            pNumDatTim = pNumDatTim / 100L;
            minute = (int) (pNumDatTim % 100L);
            pNumDatTim = pNumDatTim / 100L;
            hour = (int) (pNumDatTim % 100L);
            pNumDatTim = pNumDatTim / 100L;
        } else {
            hour = 0;
            minute = 0;
            second = 0;
            milsecond = 0;
        }

        if (pNumDatTim > 10000000 && pNumDatTim < 99999999) {
            day = (int) (pNumDatTim % 100L);
            pNumDatTim = pNumDatTim / 100L;
            month = (int) (pNumDatTim % 100L);
            pNumDatTim = pNumDatTim / 100L;
            year = (int) pNumDatTim;
        } else {
            day = 0;
            month = 0;
            year = 0;
        }

        // the day passed from 1859-01-01
        passedDay = (year - 1859) * 365 + (year - 1) / 4 - (year - 1) / 100 + (year - 1) / 400 -
                (1858 / 4 - 1858 / 100 + 1858 / 400);
        // add the day from 1858-11-17 to 1859-01-01
        passedDay = passedDay + 45;

        // add the days in passed month of current year(suppose it is not leap year)
        passedDay += month_offset[month - 1];

        // if current year is leap year and February is passed, add one day
        if (((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) && month > 2) {
            passedDay++;
        }

        // add the days in passed days of current month
        passedDay += day - 1;

        // caculte the timestamp
        retTime = (passedDay * 24 * 3600 * 100L + hour * 3600 * 100 + minute * 60 * 100 +
                second * 100 + milsecond) * 100000L;

        return retTime;
    }

    /**
     * long 转二进制
     * 注意：小端模式
     */
    public static byte[] long2bytes(long res) {
        return new byte[]{
                (byte) ((res) & 0xFF),
                (byte) ((res >> 8) & 0xFF),
                (byte) ((res >> 16) & 0xFF),
                (byte) ((res >> 24) & 0xFF),
                (byte) ((res >> 32) & 0xFF),
                (byte) ((res >> 40) & 0xFF),
                (byte) ((res >> 48) & 0xFF),
                (byte) ((res >> 56) & 0xFF)};
    }

    /**
     * int 转二进制
     * 注意：小端模式
     */
    public static byte[] int2bytes(int res) {
        return new byte[]{
                (byte) ((res) & 0xFF),
                (byte) ((res >> 8) & 0xFF),
                (byte) ((res >> 16) & 0xFF),
                (byte) ((res >> 24) & 0xFF)};
    }

    /**
     * short 转二进制
     * 注意：小端模式
     */
    public static byte[] short2bytes(int res) {
        return new byte[]{
                (byte) ((res) & 0xFF),
                (byte) ((res >> 8) & 0xFF)};
    }




}
