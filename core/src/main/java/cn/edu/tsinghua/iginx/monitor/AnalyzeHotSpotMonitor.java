package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.operator.OperatorType;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class AnalyzeHotSpotMonitor implements IMonitor {

    private static final Logger logger = LoggerFactory.getLogger(AnalyzeHotSpotMonitor.class);

    private final boolean isEnableMonitor = ConfigDescriptor.getInstance().getConfig().isEnableMonitor();
    private final List<Pair<FragmentMeta, Long>> readCostList = new CopyOnWriteArrayList<>(); // 分片,查询耗时
    private final List<Pair<FragmentMeta, Long>> writeCostList = new CopyOnWriteArrayList<>(); // 分片,写入耗时
    private final List<Pair<FragmentMeta, Long>> readCostListNoMigration = new CopyOnWriteArrayList<>(); // 分片,查询耗时
    private final List<Pair<FragmentMeta, Long>> writeCostListNoMigration = new CopyOnWriteArrayList<>(); // 分片,写入耗时
    private static final AnalyzeHotSpotMonitor instance = new AnalyzeHotSpotMonitor();

    public static AnalyzeHotSpotMonitor getInstance() {
        return instance;
    }

    public List<Pair<FragmentMeta, Long>> getReadCostList() {
        return readCostList;
    }

    public List<Pair<FragmentMeta, Long>> getWriteCostList() {
        return writeCostList;
    }

    public List<Pair<FragmentMeta, Long>> getReadCostListNoMigration() {
        return readCostListNoMigration;
    }

    public List<Pair<FragmentMeta, Long>> getWriteCostListNoMigration() {
        return writeCostListNoMigration;
    }

    public void recordWithMigration(long taskId, FragmentMeta fragmentMeta, OperatorType operatorType) {
        if (isEnableMonitor) {
            long duration = (System.nanoTime() - taskId) / 1000000;
            if (operatorType == OperatorType.Project) {
                readCostList.add(new Pair<>(fragmentMeta, duration));
            } else if (operatorType == OperatorType.Insert) {
                writeCostList.add(new Pair<>(fragmentMeta, duration));
            }
        }
    }

    public void recordWithoutMigration(long taskId, FragmentMeta fragmentMeta, OperatorType operatorType) {
        if (isEnableMonitor) {
            long duration = (System.nanoTime() - taskId) / 1000000;
            if (operatorType == OperatorType.Project) {
                readCostListNoMigration.add(new Pair<>(fragmentMeta, duration));
            } else if (operatorType == OperatorType.Insert) {
                writeCostListNoMigration.add(new Pair<>(fragmentMeta, duration));
            }
        }
    }

    @Override
    public void clear() {
        writeCostList.clear();
        readCostList.clear();
        writeCostListNoMigration.clear();
        readCostListNoMigration.clear();
    }
}