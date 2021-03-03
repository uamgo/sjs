package com.sjs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class OrderConfirm implements DataInterface {
    int order_type;                 // C1
    String acct_id;                 // C11
    long order_no;                  // I8
    String sec_code;                // C9
    String trade_dir;               // C1
    long order_price;               // I8
    long order_vol;                 // I8
    long act_no;                    // I8
    long withdraw_order_no;         // I8
    int pbu;                        // I2
    long reserved1;                 // I4
    String order_status;            // C1
    long reserved2;                 // I4
    long reserved3;                 // I4
    long ts;                        // I8
    String rsrv;                    // C3

    @Override
    public int size() {
        return 88;
    }

    @Override
    public boolean deserialize(byte[] buffer, int offset) {
        int idx = offset;

        order_type = MsgUtil.uint8FromBytes(buffer, idx);
        idx += 1;

        acct_id = new String(buffer, idx,11);
        idx += 11;

        order_no = MsgUtil.int64FromBytes(buffer, idx);
        idx += 8;

        sec_code = new String(buffer, idx,9);
        idx += 9;

        trade_dir = new String(buffer, idx,1);
        idx += 1;

        order_price = MsgUtil.int64FromBytes(buffer, idx);
        idx += 8;

        order_vol = MsgUtil.int64FromBytes(buffer, idx);
        idx += 8;

        act_no = MsgUtil.int64FromBytes(buffer, idx);
        idx += 8;

        withdraw_order_no = MsgUtil.int64FromBytes(buffer, idx);
        idx += 8;

        pbu = MsgUtil.uint16BizFromBytes(buffer, idx);
        idx += 2;

        reserved1= MsgUtil.uint32BizFromBytes(buffer, idx);
        idx += 4;

        order_status = new String(buffer, idx,1);
        idx += 1;

        reserved2 = MsgUtil.uint32BizFromBytes(buffer, idx);
        idx += 4;

        reserved3 = MsgUtil.uint32BizFromBytes(buffer, idx);
        idx += 4;

        ts = MsgUtil.int64FromBytes(buffer, idx);
        idx += 8;

        rsrv = new String(buffer, idx,3);
        idx += 3;

        return true;
    }

    @Override
    public void updateTimestamp() {
        ts = System.currentTimeMillis() * 1000000;
    }

    public int getOrder_type() {
        return order_type;
    }

    public String getAcct_id() {
        return acct_id;
    }

    public long getOrder_no() {
        return order_no;
    }

    public String getSec_code() {
        return sec_code;
    }

    public String getTrade_dir() {
        return trade_dir;
    }

    public long getOrder_price() {
        return order_price;
    }

    public long getOrder_vol() {
        return order_vol;
    }

    public long getAct_no() {
        return act_no;
    }

    public long getWithdraw_order_no() {
        return withdraw_order_no;
    }

    public int getPbu() {
        return pbu;
    }

    public long getReserved1() {
        return reserved1;
    }

    public String getOrder_status() {
        return order_status;
    }

    public long getReserved2() {
        return reserved2;
    }

    public long getReserved3() {
        return reserved3;
    }

    public long getTs() {
        return ts;
    }

    public String getRsrv() {
        return rsrv;
    }

    public String toString() {
        return String.format("%d|%s|%d|%s|%s|%d|%d|%d|%d|%d|%d|%s|%d|%d|%d|%s",
                order_type,acct_id,order_no,
                sec_code,trade_dir,order_price,
                order_vol,act_no,withdraw_order_no,
                pbu,reserved1,order_status,reserved2,
                reserved3,ts,rsrv);
    }

    public static void main(String[] args) {
        FileInputStream inData_;
        String dataFile_ = "";

        try {
            inData_ = new FileInputStream(dataFile_);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }

        OrderConfirm obj = new OrderConfirm();
        byte[] buffer = new byte[obj.size()];
        int len = 0;


        while (true) {
            try {
                len = inData_.read(buffer);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            if (len != -1 && len == obj.size()) {
                obj.deserialize(buffer, 0);
                obj.updateTimestamp();
                System.out.println(obj.toString());
            } else {
                break;
            }
        }
    }

}
