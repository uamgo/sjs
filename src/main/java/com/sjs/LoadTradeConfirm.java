package com.sjs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class LoadTradeConfirm {

    private static int total = 0;

    public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
        Class.forName("org.trafodion.jdbc.t4.T4Driver");
        final String ip = args[0];
        String dataFile_ = args[1];
        final int batchSize;
        if (args.length > 3) {
            batchSize = Integer.valueOf(args[3]);
        } else {
            batchSize = 1000;
        }
        long start = System.currentTimeMillis();
        final Random r = new Random();
        final String name = "trafodion";
        final String pwd = "traf123";
        final String sql = "upsert using load into TRAFODION.SH_EXCHANGE.TRADE_CMF values("
                + "?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?,?,?,?,?)";

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

                    long batchStart = System.currentTimeMillis();
                    //add batch here
                    //System.out.println(obj.toString());
//sec_code, act_no, acct_id,
//                order_no, trade_dir, trade_price,
//                trade_vol, ts, pbu, reserved,rsrv
                    ps.setString(1, String.valueOf(obj.buy.sec_code));
                    ps.setString(2, String.valueOf(obj.buy.act_no));
                    ps.setString(3, String.valueOf(obj.buy.acct_id));
                    ps.setString(4, String.valueOf(obj.buy.order_no));
                    ps.setString(5,obj.buy.trade_dir );
                    ps.setString(6, String.valueOf(obj.buy.trade_price));
                    ps.setString(7, String.valueOf(obj.buy.trade_vol));
                    ps.setString(8, String.valueOf(obj.buy.ts));
                    ps.setString(9, String.valueOf(obj.buy.pbu));
                    ps.setString(10, String.valueOf(obj.buy.reserved));
                    ps.setString(11, String.valueOf(obj.sell.sec_code));
                    ps.setString(12, String.valueOf(obj.sell.act_no));
                    ps.setString(13, String.valueOf(obj.sell.acct_id));
                    ps.setString(14, String.valueOf(obj.sell.order_no));
                    ps.setString(15,obj.sell.trade_dir );
                    ps.setString(16, String.valueOf(obj.sell.trade_price));
                    ps.setString(17, String.valueOf(obj.sell.trade_vol));
                    ps.setString(18, String.valueOf(obj.sell.ts));
                    ps.setString(19, String.valueOf(obj.sell.pbu));
                    ps.setString(20,String.valueOf(obj.sell.reserved));
                    ps.addBatch();
                    ++batchIndex;
                    if (batchIndex >= batchSize) {
                        ps.executeBatch();
                        batchIndex = 0;
                        long batchEnd = System.currentTimeMillis();
                    }
                }
                if (batchIndex > 0) {
                    ps.executeBatch();
                }
                else {
                    break;
                }
            }
            long batchEnd = System.currentTimeMillis();
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

}
