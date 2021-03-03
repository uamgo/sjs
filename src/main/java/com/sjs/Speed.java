package com.sjs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Random;

public class Speed {

    private static int total = 0;

    public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
        Class.forName("org.trafodion.jdbc.t4.T4Driver");
        final String ip = args[0];
        int n = Integer.parseInt(args[1]);
        final int rows = Integer.valueOf(args[2]);
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
        final String sql = "upsert using load into speed_test values("
                + "?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?)";
        Thread[] threads = new Thread[n];
        for (int i = 0; i < n; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
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

                        long start = System.currentTimeMillis();
                        int batchIndex = 0;
                        for (int i = 0; i < rows; i++) {
                            long batchStart = System.currentTimeMillis();
                            ps.setString(1, String.valueOf(i));
                            ps.setString(2, String.valueOf(i));
                            ps.setInt(3, r.nextInt());
                            ps.setString(4, String.valueOf(100));
                            ps.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
                            ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));

                            for (int j = 1; j <= 89; j++) {
                                ps.setDouble(j + 6, i);
                            }
                            ps.addBatch();
                            ++batchIndex;
                            if (batchIndex >= batchSize) {
                                ps.executeBatch();
                                batchIndex = 0;
                                long batchEnd = System.currentTimeMillis();
                                System.out.println(String
                                        .format("[%s] Total speed: %d, batch speed: %d, finished: %1$,.2f%%",
                                                Thread.currentThread().getId(),
                                                i * 1000 / (batchEnd - start),
                                                batchSize * 1000 / (batchEnd - batchStart),
                                                (double) i * 100 / rows));
                            }
                        }
                        if (batchIndex > 0) {
                            ps.executeBatch();
                        }
                        long batchEnd = System.currentTimeMillis();
                        System.out.println(String
                                .format("[%s] Total speed: %d, 100%% finished!",
                                        Thread.currentThread().getId(),
                                        rows * 1000 / (batchEnd - start)));
                        addTotal((int) (rows * 1000 / (batchEnd - start)));
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
            };
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        long end = System.currentTimeMillis();
        System.out.println(String.format("Speed in total: %d rows/s, %d secords( %d mins)", total
                , (end - start) / 1000, (end - start) / 60000));
    }

    static void addTotal(int totalRow) {
        synchronized (Speed.class) {
            total += totalRow;
        }
    }
}
