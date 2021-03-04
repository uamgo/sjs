package com.sjs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

class TradeRecord implements DataInterface {

    String sec_code;         // C9
    long act_no;             // I8
    String acct_id;          // C11
    long order_no;           // I8
    String trade_dir;        // C1
    long trade_price;        // I8
    long trade_vol;          // I8
    long ts;                 // I8
    long pbu;                // I4
    long reserved;           // I4
    String rsrv;             // C3

    @Override
    public int size() {
        return 72;
    }

    @Override
    public boolean deserialize(byte[] buffer, int offset) {
        int idx = offset;

        sec_code = new String(buffer, idx, 9);
        idx += 9;

        act_no = MsgUtil.int64FromBytes(buffer, idx);
        idx += 8;

        acct_id = new String(buffer, idx, 11);
        idx += 11;

        order_no = MsgUtil.int64FromBytes(buffer, idx);
        idx += 8;

        trade_dir = new String(buffer, idx, 1);
        idx += 1;

        trade_price = MsgUtil.int64FromBytes(buffer, idx);
        idx += 8;

        trade_vol = MsgUtil.int64FromBytes(buffer, idx);
        idx += 8;

        ts = MsgUtil.int64FromBytes(buffer, idx);
        idx += 8;

        pbu = MsgUtil.uint32BizFromBytes(buffer, idx);
        idx += 4;

        reserved = MsgUtil.uint32BizFromBytes(buffer, idx);
        idx += 4;

        rsrv = new String(buffer, idx, 3);
        rsrv = "";
        idx += 3;

        return true;
    }

    public String toString() {
        return String.format("%s|%d|%s|%d|%s|%d|%d|%d|%d|%d|%s",
            sec_code, act_no, acct_id,
            order_no, trade_dir, trade_price,
            trade_vol, ts, pbu, reserved, rsrv);
    }

    @Override
    public void updateTimestamp() {
        ts = System.currentTimeMillis() * 1000000;
    }

    public void clone(TradeRecord record) {
        sec_code = record.sec_code;
        act_no = record.act_no;
        acct_id = record.acct_id;
        order_no = record.order_no;
        trade_dir = record.trade_dir;
        trade_price = record.trade_price;
        trade_vol = record.trade_vol;
        ts = record.ts;
        pbu = record.pbu;
        reserved = record.reserved;
        rsrv = record.rsrv;
    }
}

public class TradeConfirm implements DataInterface {

    TradeRecord buy = new TradeRecord();
    TradeRecord sell = new TradeRecord();

    @Override
    public int size() {
        return buy.size() + sell.size();
    }

    @Override
    public boolean deserialize(byte[] buffer, int offset) {
        int idx = offset;
        buy.deserialize(buffer, idx);
        idx += buy.size();

        sell.deserialize(buffer, idx);
        idx += sell.size();
        return true;
    }

    @Override
    public void updateTimestamp() {
        long now = System.currentTimeMillis() * 1000000;
        buy.ts = now;
        sell.ts = now;
    }

    public TradeRecord getBuy() {
        return buy;
    }

    public TradeRecord getSell() {
        return sell;
    }

    public String toString() {
        return String.format("%s|%s", buy.toString(), sell.toString());
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

        TradeConfirm obj = new TradeConfirm();
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

    public TradeConfirm clone(TradeConfirm toClone) {
        this.buy.clone(toClone.buy);
        this.sell.clone(toClone.sell);
        return this;
    }
}
