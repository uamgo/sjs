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

        final String tradeUpsertSql = String.format(
            "upsert into %1$s  "
                + "SELECT nvl(a.account_id,b.account_id),"
                + "nvl(a.sec_code,b.sec_code),"
                + "nvl(a.num,0)+cast(nvl(b.trade_vol,'0') as int),"
                + "current_timestamp "
                + "from %1$s a right join "
                + "(values (?,?,?)) b(account_id, sec_code, trade_vol) "
                + "on a.account_id = b.account_id and a.sec_code = b.sec_code ",
            schema + "act_num");
        System.out.println(tradeUpsertSql);
        String url = "jdbc:t4jdbc://" + ip + ":23400/:";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, name, pwd);
            Statement st = conn.createStatement();
            st.execute("cqd traf_load_use_for_indexes 'OFF'");
            st.execute("cqd upd_ordered 'OFF'");
            st.execute("cqd traf_upsert_mode 'REPLACE'");
            st.execute("cqd comp_int_22 '1'");
            st.close();
            PreparedStatement ps = conn.prepareStatement(sql);
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

                    tradeUpsertPs.setString(1, String.valueOf(obj.buy.acct_id));
                    tradeUpsertPs.setString(2, String.valueOf(obj.buy.sec_code));
                    tradeUpsertPs.setString(3, String.valueOf(obj.buy.trade_vol));
                    tradeUpsertPs.addBatch();
                    tradeUpsertPs.setString(1, String.valueOf(obj.sell.acct_id));
                    tradeUpsertPs.setString(2, String.valueOf(obj.sell.sec_code));
                    tradeUpsertPs.setString(3, String.valueOf(obj.sell.trade_vol));
                    tradeUpsertPs.addBatch();
//                    System.out.println(String.format("%d,%d,%d,%d,%d,%d",
//                        String.valueOf(obj.buy.acct_id).length(),
//                        String.valueOf(obj.buy.acct_id).length(),
//                        String.valueOf(obj.buy.trade_vol).length(),
//                        String.valueOf(obj.sell.acct_id).length(),
//                        String.valueOf(obj.sell.sec_code).length(),
//                        String.valueOf(obj.sell.trade_vol).length()));

                    ++totalRows;
                    ++batchIndex;
                    if (batchIndex >= batchSize) {
                        tradeUpsertPs.executeBatch();
                        ps.executeBatch();
                        batchIndex = 0;
                        long batchEnd = System.currentTimeMillis();
                        System.out.println(
                            totalRows + " rows ," + totalRows * 1000 / (batchEnd - start)
                                + " rows/second");

                    }
                } else {
                    if (batchIndex > 0) {
                        tradeUpsertPs.executeBatch();
                        ps.executeBatch();
                        long batchEnd = System.currentTimeMillis();
                        System.out.println(
                            totalRows + " rows ," + totalRows * 1000 / (batchEnd - start)
                                + " rows/second");
                    }
                    break;
                }
            }
        } catch (SQLException e) {
            SQLException ee = e;
            while (ee != null) {
                ee.printStackTrace();
                ee = ee.getNextException();
            }
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

}
