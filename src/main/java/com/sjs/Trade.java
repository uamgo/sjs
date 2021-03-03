package com.sjs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Trade {

    private static int total = 0;

    public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
        Class.forName("org.trafodion.jdbc.t4.T4Driver");
        final String ip = args[0];
        String dataFile_ = args[1];
        final int batchSize;
        if (args.length > 2) {
            batchSize = Integer.valueOf(args[2]);
        } else {
            batchSize = 1000;
        }
        String schema;
        if (args.length > 3) {
            schema = args[3] + ".";
        } else {
            schema = "seabase.";
        }
        long start = System.currentTimeMillis();
        final Random r = new Random();
        final String name = "trafodion";
        final String pwd = "traf123";
        final String sql = "upsert using load into " + schema + "TRADE_CMF values("
            + "?,?,?,?,?,?,?,?,?,?,"
            + "?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)";
        final String tradeSql =
            "UPDATE " + schema + "act_num SET num=num+? where account_id=? and sec_code=?";
        final String tradeUpsertSql =
            "insert into " + schema + "act_num values(?,?,?,current_timestamp)";
        Set<String> keys = new HashSet<String>();
        boolean hasUpsert = false;
        boolean hasUpsertTmp = false;
        String url = "jdbc:t4jdbc://" + ip + ":23400/:";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, name, pwd);
            Statement st = conn.createStatement();
            st.execute("cqd traf_load_use_for_indexes 'OFF'");
            st.execute("cqd upd_ordered 'OFF'");
            st.execute("cqd traf_upsert_mode 'REPLACE'");
            st.close();
            PreparedStatement ps = conn.prepareStatement(sql);
            PreparedStatement tradePs = conn.prepareStatement(tradeSql);
            PreparedStatement tradeUpsertPs = conn.prepareStatement(tradeUpsertSql);
            int batchIndex = 0;
            FileInputStream inData_;

            try {
                inData_ = new FileInputStream(dataFile_);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return;
            }

            TradeConfirm obj = new TradeConfirm();
            byte[] buffer = new byte[obj.size()];
            int len = 0;
            int totalRows = 0;
            while (true) {
                try {
                    len = inData_.read(buffer);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
//                System.out.println("--len： " + len + ", obj.size: " + obj.size());
                if (len != -1 && len == obj.size()) {
                    obj.deserialize(buffer, 0);
                    obj.updateTimestamp();

                    long batchStart = System.currentTimeMillis();
                    //add batch here
                    //System.out.println(obj.toString());
//sec_code, act_no, acct_id,
//                order_no, trade_dir, trade_price,
//                trade_vol, ts, pbu, reserved,rsrv
//                    System.out.println("obj.sell.trade_dir : " + obj.sell.trade_dir);
                    ps.setString(1, String.valueOf(obj.buy.sec_code));
                    ps.setString(2, String.valueOf(obj.buy.act_no));
                    ps.setString(3, String.valueOf(obj.buy.acct_id));
                    ps.setString(4, String.valueOf(obj.buy.order_no));
                    ps.setString(5, obj.buy.trade_dir);
                    ps.setString(6, String.valueOf(obj.buy.trade_price));
                    ps.setString(7, String.valueOf(obj.buy.trade_vol));
                    ps.setString(8, String.valueOf(obj.buy.ts));
                    ps.setString(9, String.valueOf(obj.buy.pbu));
                    ps.setString(10, String.valueOf(obj.buy.reserved));
                    ps.setString(11, String.valueOf(obj.buy.rsrv));
                    ps.setString(12, String.valueOf(obj.sell.sec_code));
                    ps.setString(13, String.valueOf(obj.sell.act_no));
                    ps.setString(14, String.valueOf(obj.sell.acct_id));
                    ps.setString(15, String.valueOf(obj.sell.order_no));
                    ps.setString(16, obj.sell.trade_dir);
                    ps.setString(17, String.valueOf(obj.sell.trade_price));
                    ps.setString(18, String.valueOf(obj.sell.trade_vol));
                    ps.setString(19, String.valueOf(obj.sell.ts));
                    ps.setString(20, String.valueOf(obj.sell.pbu));
                    ps.setString(21, String.valueOf(obj.sell.reserved));
                    ps.setString(22, String.valueOf(obj.sell.rsrv));
                    ps.addBatch();

                    hasUpsertTmp = addTradeBatch(keys, tradePs, tradeUpsertPs, obj.buy.trade_vol,
                        obj.buy.sec_code,
                        obj.buy.acct_id);
                    if (!hasUpsert) {
                        hasUpsert = hasUpsertTmp;
                    }
                    hasUpsertTmp = addTradeBatch(keys, tradePs, tradeUpsertPs, obj.sell.trade_vol,
                        obj.sell.sec_code,
                        obj.sell.acct_id);
                    if (!hasUpsert) {
                        hasUpsert = hasUpsertTmp;
                    }

                    ++totalRows;
                    ++batchIndex;
                    if (batchIndex >= batchSize) {
                        if (hasUpsert) {
                            tradeUpsertPs.executeBatch();
                        }
                        ps.executeBatch();
                        tradePs.executeBatch();
                        batchIndex = 0;
                        hasUpsert = false;
                        long batchEnd = System.currentTimeMillis();
                        System.out.println(
                            totalRows + " rows ," + totalRows * 1000 / (batchEnd - start)
                                + " rows/second");

                    }
                } else {
                    if (batchIndex > 0) {
                        if (hasUpsert) {
                            tradeUpsertPs.executeBatch();
                        }
                        ps.executeBatch();
                        tradePs.executeBatch();
                        long batchEnd = System.currentTimeMillis();
                        System.out.println(
                            totalRows + " rows ," + totalRows * 1000 / (batchEnd - start)
                                + " rows/second");
                    }
                    break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static boolean addTradeBatch(Set<String> keys,
        PreparedStatement tradePs, PreparedStatement tradeUpsertPs,
        long trade_vol, String sec_code, String acct_id) throws SQLException {
        if (keys.contains(acct_id + sec_code)) {
            tradePs.setLong(1, trade_vol);
            tradePs.setString(2, String.valueOf(acct_id));
            tradePs.setString(3, String.valueOf(sec_code));
            tradePs.addBatch();
            return false;
        } else {
            tradeUpsertPs.setString(1, String.valueOf(acct_id));
            tradeUpsertPs.setString(2, String.valueOf(sec_code));
            tradeUpsertPs.setLong(3, trade_vol);
            tradeUpsertPs.addBatch();
            return true;
        }
    }

}
