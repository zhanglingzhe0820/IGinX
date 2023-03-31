package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.pool.IginxInfo;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;

public class IoTDBSessionPoolExample {
    private static final String S1 = "sg.d1.s1";
    private static final String S2 = "sg.d1.s2";
    private static final String S3 = "sg.d2.s1";
    private static final String S4 = "sg.d3.s1";
    private static final long COLUMN_START_TIMESTAMP = 1L;
    private static final long COLUMN_END_TIMESTAMP = 10000L;
    private static final long NON_ALIGNED_COLUMN_START_TIMESTAMP = 10001L;
    private static final long NON_ALIGNED_COLUMN_END_TIMESTAMP = 20000L;
    private static final long ROW_START_TIMESTAMP = 20001L;
    private static final long ROW_END_TIMESTAMP = 30000L;
    private static final long NON_ALIGNED_ROW_START_TIMESTAMP = 30001L;
    private static final long NON_ALIGNED_ROW_END_TIMESTAMP = 40000L;
    private static final int INTERVAL = 10;
    private static final boolean needMultiIginx = true;

    private static SessionPool sessionPool;

    // construct sessionpool with all iginx
    private static void constructCustomSessionPool() {
        // {"id":3,"ip":"0.0.0.0","port":6888} and {"id":2,"ip":"0.0.0.0","port":7888}
        if (needMultiIginx) {
            List<IginxInfo> iginxList =
                    new ArrayList<IginxInfo>() {
                        {
                            add(
                                    new IginxInfo.Builder()
                                            .host("0.0.0.0")
                                            .port(6888)
                                            .user("root")
                                            .password("root")
                                            .build());

                            add(
                                    new IginxInfo.Builder()
                                            .host("0.0.0.0")
                                            .port(7888)
                                            .user("root")
                                            .password("root")
                                            .build());
                        }
                    };

            sessionPool = new SessionPool(iginxList, 3);
        } else {
            sessionPool =
                    new SessionPool.Builder()
                            .host("127.0.0.1")
                            .port(6888)
                            .user("root")
                            .password("root")
                            .maxSize(3)
                            .build();
        }
    }

    public static void main(String[] args) throws SessionException, ExecutionException {

        constructCustomSessionPool();

        // 列式插入对齐数据
        insertColumnRecords();
        // 列式插入非对齐数据
        insertNonAlignedColumnRecords();
        // 行式插入对齐数据
        insertRowRecords();
        // 行式插入非对齐数据
        insertNonAlignedRowRecords();
        // 查询序列
        showTimeSeries();
        // 查询数据
        queryData();
        // 聚合查询
        aggregateQuery();
        // Last 查询
        lastQuery();
        // 降采样聚合查询
        downsampleQuery();
        // 曲线匹配
        curveMatch();
        // 删除数据
        deleteDataInColumns();
        // 再次查询数据
        queryData();
        // 查看集群信息
        showClusterInfo();

        // 关闭 Session
        sessionPool.close();
    }

    private static void insertColumnRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        int size = (int) (COLUMN_END_TIMESTAMP - COLUMN_START_TIMESTAMP + 1);
        long[] timestamps = new long[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = i + COLUMN_START_TIMESTAMP;
        }

        Object[] valuesList = new Object[4];
        for (long i = 0; i < 4; i++) {
            Object[] values = new Object[size];
            for (long j = 0; j < size; j++) {
                if (i < 2) {
                    values[(int) j] = i + j;
                } else {
                    values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.LONG);
        }
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.BINARY);
        }

        System.out.println("insertColumnRecords...");
        sessionPool.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void insertNonAlignedColumnRecords()
            throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        int size =
                (int) (NON_ALIGNED_COLUMN_END_TIMESTAMP - NON_ALIGNED_COLUMN_START_TIMESTAMP + 1);
        long[] timestamps = new long[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = i + NON_ALIGNED_COLUMN_START_TIMESTAMP;
        }

        Object[] valuesList = new Object[4];
        for (long i = 0; i < 4; i++) {
            Object[] values = new Object[size];
            for (long j = 0; j < size; j++) {
                if (j >= size - 50) {
                    values[(int) j] = null;
                } else {
                    if (i < 2) {
                        values[(int) j] = i + j;
                    } else {
                        values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                    }
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.LONG);
        }
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.BINARY);
        }

        System.out.println("insertNonAlignedColumnRecords...");
        sessionPool.insertNonAlignedColumnRecords(
                paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void insertRowRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        int size = (int) (ROW_END_TIMESTAMP - ROW_START_TIMESTAMP + 1);
        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = ROW_START_TIMESTAMP + i;
            Object[] values = new Object[4];
            for (long j = 0; j < 4; j++) {
                if (j < 2) {
                    values[(int) j] = i + j;
                } else {
                    values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.LONG);
        }
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.BINARY);
        }

        System.out.println("insertRowRecords...");
        sessionPool.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void insertNonAlignedRowRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        int size = (int) (NON_ALIGNED_ROW_END_TIMESTAMP - NON_ALIGNED_ROW_START_TIMESTAMP + 1);
        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = NON_ALIGNED_ROW_START_TIMESTAMP + i;
            Object[] values = new Object[4];
            for (long j = 0; j < 4; j++) {
                if ((i + j) % 2 == 0) {
                    values[(int) j] = null;
                } else {
                    if (j < 2) {
                        values[(int) j] = i + j;
                    } else {
                        values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                    }
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.LONG);
        }
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.BINARY);
        }

        System.out.println("insertNonAlignedRowRecords...");
        sessionPool.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void showTimeSeries() throws ExecutionException, SessionException {
        List<Column> columnList = sessionPool.showColumns();
        columnList.forEach(column -> System.out.println(column.toString()));
    }

    private static void queryData() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        long startTime = NON_ALIGNED_COLUMN_END_TIMESTAMP - 100L;
        long endTime = ROW_START_TIMESTAMP + 100L;

        SessionQueryDataSet dataSet = sessionPool.queryData(paths, startTime, endTime);
        dataSet.print();
    }

    private static void aggregateQuery() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

        long startTime = COLUMN_END_TIMESTAMP - 100L;
        long endTime = NON_ALIGNED_ROW_START_TIMESTAMP + 100L;

        // 聚合查询开始
        System.out.println("Aggregate Query: ");

        // MAX
        SessionAggregateQueryDataSet dataSet =
                sessionPool.aggregateQuery(paths, startTime, endTime, AggregateType.MAX);
        dataSet.print();

        // MIN
        dataSet = sessionPool.aggregateQuery(paths, startTime, endTime, AggregateType.MIN);
        dataSet.print();

        // FIRST_VALUE
        dataSet = sessionPool.aggregateQuery(paths, startTime, endTime, AggregateType.FIRST_VALUE);
        dataSet.print();

        // LAST_VALUE
        dataSet = sessionPool.aggregateQuery(paths, startTime, endTime, AggregateType.LAST_VALUE);
        dataSet.print();

        // COUNT
        dataSet = sessionPool.aggregateQuery(paths, startTime, endTime, AggregateType.COUNT);
        dataSet.print();

        // SUM
        dataSet = sessionPool.aggregateQuery(paths, startTime, endTime, AggregateType.SUM);
        dataSet.print();

        // AVG
        dataSet = sessionPool.aggregateQuery(paths, startTime, endTime, AggregateType.AVG);
        dataSet.print();

        // 聚合查询结束
        System.out.println("Aggregate Query Finished.");
    }

    private static void lastQuery() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        SessionQueryDataSet dataSet = sessionPool.queryLast(paths, 0L);
        dataSet.print();
    }

    private static void downsampleQuery() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

        long startTime = COLUMN_END_TIMESTAMP - 100L;
        long endTime = NON_ALIGNED_ROW_START_TIMESTAMP + 100L;

        // 降采样查询开始
        System.out.println("Downsample Query: ");

        // MAX
        SessionQueryDataSet dataSet =
                sessionPool.downsampleQuery(
                        paths, startTime, endTime, AggregateType.MAX, INTERVAL * 100L);
        dataSet.print();

        // MIN
        dataSet =
                sessionPool.downsampleQuery(
                        paths, startTime, endTime, AggregateType.MIN, INTERVAL * 100L);
        dataSet.print();

        // FIRST_VALUE
        dataSet =
                sessionPool.downsampleQuery(
                        paths, startTime, endTime, AggregateType.FIRST_VALUE, INTERVAL * 100L);
        dataSet.print();

        // LAST_VALUE
        dataSet =
                sessionPool.downsampleQuery(
                        paths, startTime, endTime, AggregateType.LAST_VALUE, INTERVAL * 100L);
        dataSet.print();

        // COUNT
        dataSet =
                sessionPool.downsampleQuery(
                        paths, startTime, endTime, AggregateType.COUNT, INTERVAL * 100L);
        dataSet.print();

        // SUM
        dataSet =
                sessionPool.downsampleQuery(
                        paths, startTime, endTime, AggregateType.SUM, INTERVAL * 100L);
        dataSet.print();

        // AVG
        dataSet =
                sessionPool.downsampleQuery(
                        paths, startTime, endTime, AggregateType.AVG, INTERVAL * 100L);
        dataSet.print();

        // 降采样查询结束
        System.out.println("Downsample Query Finished.");
    }

    private static void curveMatch() throws ExecutionException, SessionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

        long startTime = COLUMN_END_TIMESTAMP - 100L;
        long endTime = NON_ALIGNED_ROW_START_TIMESTAMP + 100L;

        double bias = 6.0;
        int queryNum = 30;
        List<Double> queryList = new ArrayList<>();
        for (int i = 0; i < queryNum; i++) {
            queryList.add(startTime + bias + i);
        }
        long curveUnit = 1L;

        CurveMatchResult result =
                sessionPool.curveMatch(paths, startTime, endTime, queryList, curveUnit);
        System.out.println(result.toString());
    }

    private static void deleteDataInColumns() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S3);
        paths.add(S4);

        long startTime = NON_ALIGNED_COLUMN_END_TIMESTAMP - 50L;
        long endTime = ROW_START_TIMESTAMP + 50L;

        sessionPool.deleteDataInColumns(paths, startTime, endTime);
    }

    public static void showClusterInfo() throws SessionException, ExecutionException {
        ClusterInfo clusterInfo = sessionPool.getClusterInfo();
        System.out.println(clusterInfo.getIginxInfos());
        System.out.println(clusterInfo.getStorageEngineInfos());
        System.out.println(clusterInfo.getMetaStorageInfos());
        System.out.println(clusterInfo.getLocalMetaStorageInfo());
    }
}
