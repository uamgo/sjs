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

public class LoadOrderConfirm {

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
        final String sql = "upsert using load into TRAFODION.SH_EXCHANGE.ORDER_CMF_CDC values("
                + "?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?)";

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

                    long batchStart = System.currentTimeMillis();
                    //add batch here
                    //System.out.println(obj.toString());
//                    order_type,acct_id,order_no,
//                            sec_code,trade_dir,order_price,
//                            order_vol,act_no,withdraw_order_no,
//                            pbu,reserved1,order_status,reserved2,
//                            reserved3,ts,rsrv
                    ps.setString(1, String.valueOf(obj.order_type));
                    ps.setString(2,obj.acct_id );
                    ps.setString(3, String.valueOf(obj.order_no));
                    ps.setString(4,obj.sec_code );
                    ps.setString(5,obj.trade_dir );
                    ps.setString(6, String.valueOf(obj.order_price));
                    ps.setString(7, String.valueOf(obj.order_vol));
                    ps.setString(8, String.valueOf(obj.act_no));
                    ps.setString(9, String.valueOf(obj.withdraw_order_no));
                    ps.setString(10, String.valueOf(obj.pbu));
                    ps.setString(11, String.valueOf(obj.reserved1));
                    ps.setString(12,obj.order_status );
                    ps.setString(13, String.valueOf(obj.reserved2));
                    ps.setString(14, String.valueOf(obj.reserved3));
                    ps.setString(15, String.valueOf(obj.ts));
                    ps.setString(16,obj.rsrv );
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
