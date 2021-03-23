package com.sjs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Trade extends Thread {

    private static String ip;
    private static String dataFile_;
    private static Integer batchSize;
    private static String schema;
    private static String usingLoad;
    private static FileInputStream inData_;
    private static long startMs;
    private static Integer threads;
    private final int id;
    private int startIndex = -1;
    private int endIndex = 0;
    private final byte[] buffer;
    private TradeConfirm[] tradeConfirms;
    private static long totalRows = 0;
    private static ReentrantLock LOCK = new ReentrantLock();
    private static Map<Integer, Trade> map = new ConcurrentHashMap<Integer, Trade>();
    private static Map<Integer, ConcurrentLinkedQueue<TradeConfirm>> tradeConfirmQueueMap = new ConcurrentHashMap<Integer, ConcurrentLinkedQueue<TradeConfirm>>();
    private static boolean endOfFile = false;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private boolean isEmpty = true;

    public Trade(int id) {
        this.id = id;
        tradeConfirms = new TradeConfirm[batchSize];
        for (int i = 0; i < batchSize; i++) {
            tradeConfirms[i] = new TradeConfirm();
        }
        this.buffer = new byte[tradeConfirms[0].size()];
        map.put(id, this);
    }

    public static void main(String[] args) throws ClassNotFoundException {
        Class.forName("org.trafodion.jdbc.t4.T4Driver");
        ip = args[0];
        dataFile_ = args[1];
        if (args.length > 2) {
            batchSize = Integer.valueOf(args[2]);
        } else {
            batchSize = 1000;
        }
        if (args.length > 3) {
            schema = args[3] + ".";
        } else {
            schema = "seabase.";
        }
        usingLoad = "";
        if (args.length > 4) {
            if ("load".equals(args[4])) {
                usingLoad = "using load";
            }
        }

        if (args.length > 5) {
            threads = Integer.valueOf(args[5]);
        } else {
            threads = 1;
        }
        for (int i = 0; i < threads; i++) {
            tradeConfirmQueueMap.put(i,
                new ConcurrentLinkedQueue<TradeConfirm>());
        }
        Thread readFileThread = new Thread() {
            @Override
            public void run() {
                try {
                    inData_ = new FileInputStream(dataFile_);
                    Queue<TradeConfirm> queue;
                    do {
                        TradeConfirm tmpTradeConfirm = new TradeConfirm();
                        int len = inData_.read(bufferG);
                        if (len != -1 && len == tmpTradeConfirm.size()) {
                            tmpTradeConfirm.deserialize(bufferG, 0);
                            tmpTradeConfirm.updateTimestamp();
                            queue = tradeConfirmQueueMap
                                .get(hashSecCode(tmpTradeConfirm.buy.sec_code));
                            queue.offer(tmpTradeConfirm);
                            if (queue.size() > 10000) {
                                sleep(100);
                            }
                        } else {
                            endOfFile = true;
                        }

                    } while (!endOfFile);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                    return;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        startMs = System.currentTimeMillis();
        ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
        pool.submit(readFileThread);
        for (int i = 0; i < threads; i++) {
            pool.submit(new Trade(i));
        }
        try {
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static int total = 0;

    public void run() {
        final String name = "trafodion";
        final String pwd = "traf123";
        final String sql = "upsert using load into " + schema + "TRADE_CMF values("
            + "?,?,?,?,?,?,?,?,?,?,"
            + "?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)";

        final String tradeUpsertSql = String.format(
            "upsert %2$s into %1$s  "
                + "SELECT nvl(a.account_id,b.account_id),"
                + "nvl(a.sec_code,b.sec_code),"
                + "nvl(a.num,0)+cast(b.trade_vol as int),"
                + "current_timestamp "
                + "from %1$s a <<+cardinality 1e8>> right join "
                + "(values (?,?,?)) b(account_id, sec_code, trade_vol) "
                + "on a.account_id = b.account_id and a.sec_code = b.sec_code ",
            schema + "act_num", usingLoad);
        System.out.println(tradeUpsertSql);
        String url = "jdbc:t4jdbc://" + ip + ":23400/:";
        Map<String, String[]> dataMap = new HashMap<String, String[]>(2 * batchSize);
        String[][] dataBuffer = new String[2 * batchSize][];
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

            while (true) {
                TradeConfirm obj = read();
                if (obj != null) {
                    long batchStart = System.currentTimeMillis();
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
                    ps.setNull(11, Types.INTEGER);
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
                    ps.setNull(22, Types.INTEGER);
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
                        dataBuffer[batchSize + batchIndex][1] = String
                            .valueOf(obj.sell.sec_code)
                            .trim();
                        dataBuffer[batchSize + batchIndex][2] = String
                            .valueOf(0 - obj.sell.trade_vol);
                        dataMap.put(key, dataBuffer[batchSize + batchIndex]);
                    }

                    ++batchIndex;
                    if (batchIndex >= batchSize) {
                        executeBatch(conn, tradeUpsertPs, ps, dataMap, batchIndex);
                        batchIndex = 0;
                    }
                    continue;
                } else if (endOfFile) {
                    if (batchIndex > 0) {
                        executeBatch(conn, tradeUpsertPs, ps, dataMap, batchIndex);
                    }
                    break;
                }
            }
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            SQLException ee = e;
            while (ee != null) {
                ee.printStackTrace();
                ee = ee.getNextException();
            }
        } catch (Exception e) {
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

    private void executeBatch(Connection conn, PreparedStatement tradeUpsertPs,
        PreparedStatement ps, Map<String, String[]> dataMap, int batchIndex) throws SQLException {
        for (String[] values : dataMap.values()) {
            tradeUpsertPs.setString(1, values[0]);
            tradeUpsertPs.setString(2, values[1]);
            tradeUpsertPs.setString(3, values[2]);
            tradeUpsertPs.addBatch();
        }
        tradeUpsertPs.executeBatch();
        dataMap.clear();
        ps.executeBatch();
        log(batchIndex);
        conn.commit();
        this.countDownLatch.countDown();
    }

    private void log(int numRows) {
        synchronized (Trade.class) {
            totalRows += numRows;

            long end = System.currentTimeMillis();
            String log = String
                .format("Thread[%d][batchRows:%d] %d transactions in total, TPS: %d ",
                    this.id, numRows, totalRows,
                    (totalRows * 1000 / (end - startMs)));
            System.out.println(log);
        }
    }

    private static TradeConfirm tmpTradeConfirm = new TradeConfirm();
    private static byte[] bufferG = new byte[tmpTradeConfirm.size()];

    private TradeConfirm read() {
        if (tradeConfirmQueueMap.get(id).size() == 0 && endOfFile) {
            return null;
        }
        TradeConfirm tradeConfirm = tradeConfirmQueueMap.get(id).poll();
        if (tradeConfirm == null) {
            try {
                sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return tradeConfirm;
    }

    private TradeConfirm currTradeConfirm = new TradeConfirm();

    private TradeConfirm getTradeConfirm(TradeConfirm tmpTradeConfirm) {
        return currTradeConfirm.clone(tmpTradeConfirm);
    }

    private boolean hasBufferData() {
        return !this.isEmpty;
    }

    private static Integer hashSecCode(String sec_code) {
        return Integer.valueOf(sec_code.trim()) % threads;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public TradeConfirm addTradeConfirm(TradeConfirm toClone) {
        if (!this.isEmpty && this.endIndex == this.startIndex) {
            this.countDownLatch = new CountDownLatch(1);
            try {
                this.countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        int tmpIndex = this.endIndex + 1;
        if (tmpIndex >= this.tradeConfirms.length) {
            tmpIndex = 0;
        }
        TradeConfirm tmp = this.tradeConfirms[endIndex].clone(toClone);
        this.endIndex = tmpIndex;
        this.isEmpty = false;
        return tmp;
    }

    public TradeConfirm getStartTradeConfirm() {
        TradeConfirm tmp = null;
        try {
            if (startIndex == -1) {
                startIndex = 0;
            }
            tmp = this.tradeConfirms[startIndex];
            int tmpIndex = startIndex + 1;
            if (tmpIndex >= this.tradeConfirms.length) {
                tmpIndex = 0;
            }
            startIndex = tmpIndex;
            if (tmpIndex == endIndex) {
                this.isEmpty = true;
            }
        } finally {
            this.countDownLatch.countDown();
        }

        return tmp;
    }
}
