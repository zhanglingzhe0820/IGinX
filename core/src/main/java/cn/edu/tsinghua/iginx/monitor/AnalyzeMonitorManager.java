package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

public class AnalyzeMonitorManager implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(AnalyzeMonitorManager.class);

    private static final int interval = ConfigDescriptor.getInstance().getConfig()
        .getLoadBalanceCheckInterval();

    private static AnalyzeMonitorManager INSTANCE;

    public static AnalyzeMonitorManager getInstance() {
        if (INSTANCE == null) {
            synchronized (AnalyzeMonitorManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new AnalyzeMonitorManager();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    public void run() {
        while (true) {
            try {
                //清空节点信息
                AnalyzeRequestsMonitor.getInstance().clear();
                AnalyzeHotSpotMonitor.getInstance().clear();
                Thread.sleep(interval * 1000L);
                Map<FragmentMeta, Long> writeRequestsMap = AnalyzeRequestsMonitor.getInstance()
                    .getWriteRequestsMap();
                Map<FragmentMeta, Long> readRequestsMap = AnalyzeRequestsMonitor.getInstance()
                    .getReadRequestsMap();
                Map<FragmentMeta, Long> writeHeatMap = getHeatMap(new ArrayList<>(AnalyzeHotSpotMonitor.getInstance().getWriteCostList()));
                Map<FragmentMeta, Long> readHeatMap = getHeatMap(new ArrayList<>(AnalyzeHotSpotMonitor.getInstance().getReadCostList()));
                Map<Long, List<FragmentMeta>> fragmentOfEachNode = loadFragmentOfEachNode(writeHeatMap, readHeatMap);
                for (Entry<Long, List<FragmentMeta>> fragmentOfEachNodeEntry : fragmentOfEachNode.entrySet()) {
                    long writeHeat = 0;
                    long readHeat = 0;
                    long writeRequests = 0;
                    long readRequests = 0;
                    List<FragmentMeta> fragmentMetas = fragmentOfEachNodeEntry.getValue();
                    for (FragmentMeta fragmentMeta : fragmentMetas) {
                        writeHeat += writeHeatMap.getOrDefault(fragmentMeta, 0L);
                        readHeat += readHeatMap.getOrDefault(fragmentMeta, 0L);
                        writeRequests += writeRequestsMap.getOrDefault(fragmentMeta, 0L);
                        readRequests += readRequestsMap.getOrDefault(fragmentMeta, 0L);
                    }
                    logger.error("write heat with migration of node {} : {}", fragmentOfEachNodeEntry.getKey(), writeHeat);
                    logger.error("read heat with migration of node {} : {}", fragmentOfEachNodeEntry.getKey(), readHeat);
                    logger.error("write requests with migration of node {} : {}", fragmentOfEachNodeEntry.getKey(), writeRequests);
                    logger.error("read requests with migration of node {} : {}", fragmentOfEachNodeEntry.getKey(), readRequests);
                }

                Map<FragmentMeta, Long> writeRequestsNoMigrationMap = AnalyzeRequestsMonitor.getInstance()
                    .getWriteRequestsNoMigrationMap();
                Map<FragmentMeta, Long> readRequestsNoMigrationMap = AnalyzeRequestsMonitor.getInstance()
                    .getReadRequestsNoMigrationMap();
                Map<FragmentMeta, Long> writeHeatNoMigrationMap = getHeatMap(new ArrayList<>(AnalyzeHotSpotMonitor.getInstance().getWriteCostListNoMigration()));
                Map<FragmentMeta, Long> readHeatNoMigrationMap = getHeatMap(new ArrayList<>(AnalyzeHotSpotMonitor.getInstance().getReadCostListNoMigration()));
                Map<Long, List<FragmentMeta>> fragmentOfEachNodeNoMigration = loadFragmentOfEachNode(writeHeatNoMigrationMap, readHeatNoMigrationMap);
                for (Entry<Long, List<FragmentMeta>> fragmentOfEachNodeEntry : fragmentOfEachNodeNoMigration.entrySet()) {
                    long writeHeat = 0;
                    long readHeat = 0;
                    long writeRequests = 0;
                    long readRequests = 0;
                    List<FragmentMeta> fragmentMetas = fragmentOfEachNodeEntry.getValue();
                    for (FragmentMeta fragmentMeta : fragmentMetas) {
                        writeHeat += writeHeatNoMigrationMap.getOrDefault(fragmentMeta, 0L);
                        readHeat += readHeatNoMigrationMap.getOrDefault(fragmentMeta, 0L);
                        writeRequests += writeRequestsNoMigrationMap.getOrDefault(fragmentMeta, 0L);
                        readRequests += readRequestsNoMigrationMap.getOrDefault(fragmentMeta, 0L);
                    }
                    logger.error("write heat without migration of node {} : {}", fragmentOfEachNodeEntry.getKey(), writeHeat);
                    logger.error("read heat without migration of node {} : {}", fragmentOfEachNodeEntry.getKey(), readHeat);
                    logger.error("write requests without migration of node {} : {}", fragmentOfEachNodeEntry.getKey(), writeRequests);
                    logger.error("read requests without migration of node {} : {}", fragmentOfEachNodeEntry.getKey(), readRequests);
                }
                logger.error("curr fragment num: {}", DefaultMetaManager.getInstance().getFragments().size());
            } catch (Exception e) {
                logger.error("analyze monitor manager error ", e);
            }
        }
    }

    private Map<Long, List<FragmentMeta>> loadFragmentOfEachNode(
        Map<FragmentMeta, Long> fragmentHeatWriteMap, Map<FragmentMeta, Long> fragmentHeatReadMap) {
        Set<FragmentMeta> fragmentMetaSet = new HashSet<>();
        Map<Long, List<FragmentMeta>> result = new HashMap<>();
        fragmentMetaSet.addAll(fragmentHeatWriteMap.keySet());
        fragmentMetaSet.addAll(fragmentHeatReadMap.keySet());

        for (FragmentMeta fragmentMeta : fragmentMetaSet) {
            fragmentMetaSet.add(fragmentMeta);
            List<FragmentMeta> fragmentMetas = result
                .computeIfAbsent(fragmentMeta.getMasterStorageUnit().getStorageEngineId(),
                    k -> new ArrayList<>());
            fragmentMetas.add(fragmentMeta);
        }

        List<StorageEngineMeta> storageEngineMetas = DefaultMetaManager.getInstance().getStorageEngineList();
        Set<Long> storageIds = result.keySet();
        for (StorageEngineMeta storageEngineMeta : storageEngineMetas) {
            if (!storageIds.contains(storageEngineMeta.getId())) {
                result.put(storageEngineMeta.getId(), new ArrayList<>());
            }
        }
        return result;
    }

    private Map<FragmentMeta, Long> getHeatMap(List<Pair<FragmentMeta, Long>> soredCostList) {
        Map<FragmentMeta, Long> result = new HashMap<>();
        for (Pair<FragmentMeta, Long> pair : soredCostList) {
            long heat = result.getOrDefault(pair.getK(), 0L);
            result.put(pair.getK(), heat + pair.getV());
        }
        return result;
    }
}