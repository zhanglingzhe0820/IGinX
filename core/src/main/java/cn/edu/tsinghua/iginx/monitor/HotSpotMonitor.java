package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.operator.OperatorType;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HotSpotMonitor implements IMonitor {

    private static final Logger logger = LoggerFactory.getLogger(HotSpotMonitor.class);

    private final boolean isEnableMonitor = ConfigDescriptor.getInstance().getConfig().isEnableMonitor();
    private final List<Pair<FragmentMeta, Long>> readCostList = new ArrayList<>(); // 分片,查询耗时
    private final List<Pair<FragmentMeta, Long>> writeCostList = new ArrayList<>(); // 分片,写入耗时
    private static final HotSpotMonitor instance = new HotSpotMonitor();

    public static HotSpotMonitor getInstance() {
        return instance;
    }

    public List<Pair<FragmentMeta, Long>> getReadCostList() {
        return readCostList;
    }

    public List<Pair<FragmentMeta, Long>> getWriteCostList() {
        return writeCostList;
    }

    public void recordAfter(long taskId, FragmentMeta fragmentMeta, OperatorType operatorType) {
        if (isEnableMonitor) {
            long duration = (System.nanoTime() - taskId) / 1000000;
            if (operatorType == OperatorType.Project) {
                readCostList.add(new Pair<>(fragmentMeta, duration));
            } else if (operatorType == OperatorType.Insert) {
                writeCostList.add(new Pair<>(fragmentMeta, duration));
            }
        }
    }

    @Override
    public void clear() {
        writeCostList.clear();
        readCostList.clear();
    }
}