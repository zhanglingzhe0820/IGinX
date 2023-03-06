package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.pool.SessionPool;

import java.util.ArrayList;
import java.util.List;

public class Query {

    private static List<String> timeseriesPaths = new ArrayList<>();
    private static long pointsNum;
    private static SessionPool sessionPool;

    private static void constructCustomSessionPool() {
        sessionPool =
            new SessionPool.Builder()
                .host("172.16.17.21")
                .port(6888)
                .user("root")
                .password("root")
                .maxSize(100)
                .build();
    }

    public static void main(String[] args) throws SessionException, ExecutionException, InterruptedException {
        int threadNum = Integer.parseInt(args[0]);
        int loopNum = Integer.parseInt(args[1]);
        constructCustomSessionPool();
        // 查询时间序列
        showTimeseries();

        // 查询数据
        int avgPathNum = timeseriesPaths.size() / threadNum;
        for (int i = 0; i < loopNum; i++) {
            List<Thread> queryThreads = new ArrayList<>();
            for (int j = 0; j < threadNum - 1; j++) {
                Thread queryThread = new QueryThread(timeseriesPaths.subList(j * avgPathNum, (j + 1) * avgPathNum));
                queryThreads.add(queryThread);
            }
            Thread queryThread = new QueryThread(timeseriesPaths.subList((threadNum - 1) * avgPathNum, timeseriesPaths.size()));
            queryThreads.add(queryThread);
            for (Thread thread : queryThreads) {
                thread.start();
            }
            for (Thread thread : queryThreads) {
                thread.join();
            }
            System.out.printf("loop %d end with point num: %d%n", i, pointsNum);
            pointsNum = 0;
        }

        // 关闭 Session
        sessionPool.close();
    }

    private static void showTimeseries() throws SessionException, ExecutionException {
        List<Column> columns = sessionPool.showColumns();
        for (Column column : columns) {
            timeseriesPaths.add(column.getPath());
        }
    }

    public static class QueryThread extends Thread {

        private final List<String> timeseriesPaths;

        public QueryThread(List<String> timeseriesPaths) {
            this.timeseriesPaths = timeseriesPaths;
        }

        @Override
        public void run() {
            try {
                pointsNum += queryData();
            } catch (SessionException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        private long queryData() throws SessionException, ExecutionException {
            long points = 0;
            for (String timeseries : timeseriesPaths) {
                long startTime = System.currentTimeMillis();
                List<String> queryTimeseries = new ArrayList<>();
                queryTimeseries.add(timeseries);
                SessionQueryDataSet dataSet = sessionPool.queryData(queryTimeseries, 0, Long.MAX_VALUE);
                points += (long) dataSet.getPaths().size() * dataSet.getKeys().length;
                System.out.printf("query timeseries %s consumption time %d ms with point num %d%n", timeseries, (System.currentTimeMillis() - startTime), dataSet.getPaths().size() * dataSet.getKeys().length);
            }
            return points;
        }
    }
}
