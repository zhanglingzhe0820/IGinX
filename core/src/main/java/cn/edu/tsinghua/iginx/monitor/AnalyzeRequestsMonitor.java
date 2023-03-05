package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.OperatorType;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AnalyzeRequestsMonitor implements IMonitor {

    private final boolean isEnableMonitor = ConfigDescriptor.getInstance().getConfig()
        .isEnableMonitor();
    private final Map<FragmentMeta, Long> writeRequestsMap = new ConcurrentHashMap<>(); // 数据分区->请求个数
    private final Map<FragmentMeta, Long> readRequestsMap = new ConcurrentHashMap<>(); // 数据分区->请求个数
    private final Map<FragmentMeta, Long> writeRequestsNoMigrationMap = new ConcurrentHashMap<>(); // 数据分区->请求个数
    private final Map<FragmentMeta, Long> readRequestsNoMigrationMap = new ConcurrentHashMap<>(); // 数据分区->请求个数
    private static final AnalyzeRequestsMonitor instance = new AnalyzeRequestsMonitor();

    public static AnalyzeRequestsMonitor getInstance() {
        return instance;
    }

    public Map<FragmentMeta, Long> getWriteRequestsMap() {
        return writeRequestsMap;
    }

    public Map<FragmentMeta, Long> getReadRequestsMap() {
        return readRequestsMap;
    }

    public Map<FragmentMeta, Long> getWriteRequestsNoMigrationMap() {
        return writeRequestsNoMigrationMap;
    }

    public Map<FragmentMeta, Long> getReadRequestsNoMigrationMap() {
        return readRequestsNoMigrationMap;
    }

    public void recordWithMigration(FragmentMeta fragmentMeta, Operator operator) {
        if (isEnableMonitor) {
            if (operator.getType() == OperatorType.Insert) {
                Insert insert = (Insert) operator;
                long count = writeRequestsMap.getOrDefault(fragmentMeta, 0L);
                count += (long) insert.getData().getPathNum() * insert.getData().getTimeSize();
                writeRequestsMap.put(fragmentMeta, count);
            } else if (operator.getType() == OperatorType.Project) {
                long count = readRequestsMap.getOrDefault(fragmentMeta, 0L);
                count++;
                readRequestsMap.put(fragmentMeta, count);
            }
        }
    }

    public void recordWithoutMigration(FragmentMeta fragmentMeta, Operator operator) {
        if (isEnableMonitor) {
            if (operator.getType() == OperatorType.Insert) {
                Insert insert = (Insert) operator;
                long count = writeRequestsNoMigrationMap.getOrDefault(fragmentMeta, 0L);
                count += (long) insert.getData().getPathNum() * insert.getData().getTimeSize();
                writeRequestsNoMigrationMap.put(fragmentMeta, count);
            } else if (operator.getType() == OperatorType.Project) {
                long count = readRequestsNoMigrationMap.getOrDefault(fragmentMeta, 0L);
                count++;
                readRequestsNoMigrationMap.put(fragmentMeta, count);
            }
        }
    }

    @Override
    public void clear() {
        writeRequestsMap.clear();
        readRequestsMap.clear();
        writeRequestsNoMigrationMap.clear();
        readRequestsNoMigrationMap.clear();
    }
}