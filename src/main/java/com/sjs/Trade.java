package com.sjs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class Trade {

    private static int total = 0;

    public static void main(String[] args)
        throws ClassNotFoundException, InterruptedException, SQLException {
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
        String usingLoad;
        if (args.length > 4) {
            usingLoad = "using load";
        } else {
            usingLoad = "";
        }
        long start = System.currentTimeMillis();
        final Random r = new Random();
        final String name = "trafodion";
        final String pwd = "traf123";
        final String sql = "upsert using load into " + schema + "TRADE_CMF values("
            + "?,?,?,?,?,?,?,?,?,?,"
            + "?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)";

        final String tradeUpsertSql = String.format(
            "upsert %2$s into %1$s  "
                + "SELECT nvl(a.account_id,b.account_id),"
                + "nvl(a.sec_code,b.sec_code),"
                + "nvl(a.num,0)+cast(nvl(b.trade_vol,'0') as int),"
                + "current_timestamp "
                + "from %1$s a right join "
                + "(values (?,?,?)) b(account_id, sec_code, trade_vol) "
                + "on a.account_id = b.account_id and a.sec_code = b.sec_code ",
            schema + "act_num", usingLoad);
        System.out.println(tradeUpsertSql);
        String url = "jdbc:t4jdbc://" + ip + ":23400/:";
        Map<String, String[]> dataMap = new HashMap<String, String[]>(2*batchSize);
        String[][] dataBuffer = new String[2*batchSize][];
        for (int i = 0; i < 2 * batchSize; i++) {
            dataBuffer[i] = new String[3];
        }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, name, pwd);
            Statement st = conn.createStatement();
            st.execute("cqd traf_load_use_for_indexes 'OFF'");
            st.execute("cqd upd_ordered 'OFF'");
            st.execute("cqd traf_upsert_mode 'REPLACE'");
            st.execute("cqd comp_int_22 '1'");
            st.close();
            conn.setAutoCommit(false);
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
            long totalRows = 0;
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
                    ps.setString(1, String.valueOf(obj.buy.sec_code).trim());
                    ps.setString(2, String.valueOf(obj.buy.act_no));
                    ps.setString(3, String.valueOf(obj.buy.acct_id).trim());
                    ps.setString(4, String.valueOf(obj.buy.order_no));
                    ps.setString(5, obj.buy.trade_dir);
                    ps.setString(6, String.valueOf(obj.buy.trade_price));
                    ps.setString(7, String.valueOf(obj.buy.trade_vol).trim());
                    ps.setString(8, String.valueOf(obj.buy.ts));
                    ps.setString(9, String.valueOf(obj.buy.pbu));
                    ps.setString(10, String.valueOf(obj.buy.reserved));
                    ps.setString(11, String.valueOf(obj.buy.rsrv));
                    ps.setString(12, String.valueOf(obj.sell.sec_code).trim());
                    ps.setString(13, String.valueOf(obj.sell.act_no));
                    ps.setString(14, String.valueOf(obj.sell.acct_id).trim());
                    ps.setString(15, String.valueOf(obj.sell.order_no));
                    ps.setString(16, obj.sell.trade_dir);
                    ps.setString(17, String.valueOf(obj.sell.trade_price));
                    ps.setString(18, String.valueOf(obj.sell.trade_vol).trim());
                    ps.setString(19, String.valueOf(obj.sell.ts));
                    ps.setString(20, String.valueOf(obj.sell.pbu));
                    ps.setString(21, String.valueOf(obj.sell.reserved));
                    ps.setString(22, String.valueOf(obj.sell.rsrv));
                    ps.addBatch();
                    String key = obj.buy.acct_id.replaceFirst("(\\d+).*", "$1") +
                        obj.buy.sec_code.replaceFirst("(\\d+).*", "$1");
                    if (dataMap.containsKey(key)) {
                        dataMap.get(key)[2] = String
                            .valueOf(Long.valueOf(dataMap.get(key)[2]) + obj.buy.trade_vol);
                    } else {
                        dataBuffer[batchIndex][0] = String.valueOf(obj.buy.acct_id).trim();
                        dataBuffer[batchIndex][1] = String.valueOf(obj.buy.sec_code).trim();
                        dataBuffer[batchIndex][2] = String.valueOf(obj.buy.trade_vol);
                        dataMap.put(key, dataBuffer[batchIndex]);
                    }

                    key = obj.sell.acct_id.replaceFirst("(\\d+).*", "$1") +
                        obj.sell.sec_code.replaceFirst("(\\d+).*", "$1");
                    if (dataMap.containsKey(key)) {
                        dataMap.get(key)[2] = String
                            .valueOf(Long.valueOf(dataMap.get(key)[2]) - obj.sell.trade_vol);
                    } else {
                        dataBuffer[batchSize + batchIndex][0] = String.valueOf(obj.sell.acct_id)
                            .trim();
                        dataBuffer[batchSize + batchIndex][1] = String.valueOf(obj.sell.sec_code)
                            .trim();
                        dataBuffer[batchSize + batchIndex][2] = String
                            .valueOf(0 - obj.sell.trade_vol);
                        dataMap.put(key, dataBuffer[batchSize + batchIndex]);
                    }

                    ++totalRows;
                    ++batchIndex;
                    if (batchIndex >= batchSize) {
                        for (String[] values : dataMap.values()) {
                            tradeUpsertPs.setString(1, values[0]);
                            tradeUpsertPs.setString(2, values[1]);
                            tradeUpsertPs.setString(3, values[2]);
                            tradeUpsertPs.addBatch();
                        }
                        tradeUpsertPs.executeBatch();
                        dataMap.clear();

                        ps.executeBatch();
                        batchIndex = 0;
                        long batchEnd = System.currentTimeMillis();
                        System.out.println(
                            totalRows + " rows ," + (totalRows * 1000 / (batchEnd - start))
                                + " rows/second");
                        conn.commit();
                    }
                } else {
                    if (batchIndex > 0) {
                        for (String[] values : dataMap.values()) {
                            tradeUpsertPs.setString(1, values[0]);
                            tradeUpsertPs.setString(2, values[1]);
                            tradeUpsertPs.setString(3, values[2]);
                            tradeUpsertPs.addBatch();
                        }
                        tradeUpsertPs.executeBatch();
                        dataMap.clear();
                        ps.executeBatch();
                        long batchEnd = System.currentTimeMillis();
                        System.out.println(
                            totalRows + " rows ," + (totalRows * 1000 / (batchEnd - start))
                                + " rows/second");
                        conn.commit();
                    }
                    break;
                }
            }
        } catch (SQLException e) {
            if (conn != null) {
                conn.rollback();
            }
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
