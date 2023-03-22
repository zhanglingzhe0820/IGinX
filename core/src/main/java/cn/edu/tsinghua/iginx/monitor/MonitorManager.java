package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.compaction.CompactionManager;
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

public class MonitorManager implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MonitorManager.class);

    private static final int interval = ConfigDescriptor.getInstance().getConfig()
        .getLoadBalanceCheckInterval();
    private static final double unbalanceThreshold = ConfigDescriptor.getInstance().getConfig()
        .getUnbalanceThreshold();
    private final IPolicy policy = PolicyManager.getInstance()
        .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

    private final int fragmentClearTime = 10;
    private long currLoopTime = 0;

    private final boolean isAdjustByAverageLatency = true;

    private boolean isScaleIn = false;

    private final IMetaManager metaManager = DefaultMetaManager.getInstance();
    private final CompactionManager compactionManager = CompactionManager.getInstance();
    private static MonitorManager INSTANCE;

    public static MonitorManager getInstance() {
        if (INSTANCE == null) {
            synchronized (MonitorManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new MonitorManager();
                }
            }
        }
        return INSTANCE;
    }

    public boolean scaleInStorageEngines(List<StorageEngineMeta> storageEngineMetas) {
        try {
            //如果上一轮负载均衡还在继续 必须要等待其结束
            while (DefaultMetaManager.getInstance().isResharding()) {
                Thread.sleep(1000);
            }
            isScaleIn = true;
            logger.error("scaleInStorageEngines = {}", storageEngineMetas);

            //发起负载均衡判断
            metaManager.updateFragmentRequests(RequestsMonitor.getInstance().getWriteRequestsMap(),
                RequestsMonitor.getInstance()
                    .getReadRequestsMap());

            metaManager.submitMaxActiveEndTime();
            List<Pair<FragmentMeta, Long>> writeCostList = new ArrayList<>(HotSpotMonitor.getInstance().getWriteCostList());
            writeCostList.sort(Comparator.comparing(Pair::getV));
            List<Pair<FragmentMeta, Long>> readCostList = new ArrayList<>(HotSpotMonitor.getInstance().getReadCostList());
            readCostList.sort(Comparator.comparing(Pair::getV));

            metaManager.updateFragmentHeat(filterAndGetHeatMap(writeCostList), filterAndGetHeatMap(readCostList));
            //等待收集完成
            Thread.sleep(1000);

            //集中信息（初版主要是统计分区热度）
            Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> fragmentHeatPair = metaManager
                .loadFragmentHeat();
            Map<FragmentMeta, Long> fragmentHeatWriteMap = fragmentHeatPair.getK();
            Map<FragmentMeta, Long> fragmentHeatReadMap = fragmentHeatPair.getV();
            if (fragmentHeatWriteMap == null) {
                fragmentHeatWriteMap = new HashMap<>();
            }
            if (fragmentHeatReadMap == null) {
                fragmentHeatReadMap = new HashMap<>();
            }
            Map<FragmentMeta, Long> fragmentMetaPointsMap = metaManager.loadFragmentPoints();
            List<Long> toScaleInNodes = new ArrayList<>();
            for (StorageEngineMeta storageEngineMeta : storageEngineMetas) {
                toScaleInNodes.add(storageEngineMeta.getId());
            }
            Map<Long, List<FragmentMeta>> fragmentOfEachNode = loadFragmentOfEachNodeWithColdFragment(
                fragmentHeatWriteMap, fragmentHeatReadMap, toScaleInNodes);

            logger.error("start to scale in nodes = {}", toScaleInNodes);
            logger.error("start to scale in fragmentOfEachNode = {}", fragmentOfEachNode);
//            if (DefaultMetaManager.getInstance().executeReshard()) {
            //发起负载均衡
            policy.executeReshardAndMigration(fragmentMetaPointsMap, fragmentOfEachNode,
                fragmentHeatWriteMap, fragmentHeatReadMap, toScaleInNodes);
            metaManager.scaleInStorageEngines(storageEngineMetas);
            for (StorageEngineMeta meta : storageEngineMetas) {
                PhysicalEngineImpl.getInstance().getStorageManager().removeStorage(meta);
            }
            metaManager.clearMonitors();
            return true;
//            }
//            return false;
        } catch (Exception e) {
            logger.error("execute scale-in reshard failed :", e);
            return false;
        } finally {
            //完成一轮负载均衡
//            DefaultMetaManager.getInstance().doneReshard();
            isScaleIn = false;
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                //清空节点信息
                currLoopTime++;
                if (currLoopTime % fragmentClearTime == 0) {
                    compactionManager.clearFragment();
                }
                metaManager.clearMonitors();
                Thread.sleep(interval * 1000L);
                if (isScaleIn) {
                    continue;
                }

                //发起负载均衡判断
                metaManager.updateFragmentRequests(RequestsMonitor.getInstance().getWriteRequestsMap(),
                    RequestsMonitor.getInstance()
                        .getReadRequestsMap());

                long totalWriteRequests = 0;
//        logger.error("start to print all requests of each fragments");
                Map<FragmentMeta, Long> writeRequestsMap = RequestsMonitor.getInstance()
                    .getWriteRequestsMap();
                Map<FragmentMeta, Long> readRequestsMap = RequestsMonitor.getInstance()
                    .getReadRequestsMap();
                for (Entry<FragmentMeta, Long> requestsOfEachFragment : writeRequestsMap
                    .entrySet()) {
                    totalWriteRequests += requestsOfEachFragment.getValue();
//          logger.error("fragment requests: {} = {}", requestsOfEachFragment.getKey().toString(),
//              requestsOfEachFragment.getValue());
                }
//        logger.error("end print all requests of each fragments");
//        logger.error("total write requests: {}", totalWriteRequests);

                metaManager.submitMaxActiveEndTime();
                List<Pair<FragmentMeta, Long>> writeCostList = new ArrayList<>(HotSpotMonitor.getInstance().getWriteCostList());
                writeCostList.sort(Comparator.comparing(Pair::getV));
                List<Pair<FragmentMeta, Long>> readCostList = new ArrayList<>(HotSpotMonitor.getInstance().getReadCostList());
                readCostList.sort(Comparator.comparing(Pair::getV));

                metaManager.updateFragmentHeat(filterAndGetHeatMap(writeCostList), filterAndGetHeatMap(readCostList));
                //等待收集完成
                Thread.sleep(1000);
//                while (!metaManager.isAllMonitorsCompleteCollection()) {
//                    Thread.sleep(1000);
//                }

                // 为了性能和方便，当前仅第一个节点可进行负载均衡及判断
                if (DefaultMetaManager.getInstance().getIginxList().get(0).getId() == DefaultMetaManager.getInstance().getIginxId()) {
                    //集中信息（初版主要是统计分区热度）
                    Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> fragmentHeatPair = metaManager
                        .loadFragmentHeat();
                    Map<FragmentMeta, Long> fragmentHeatWriteMap = fragmentHeatPair.getK();
                    Map<FragmentMeta, Long> fragmentHeatReadMap = fragmentHeatPair.getV();
                    if (fragmentHeatWriteMap == null) {
                        fragmentHeatWriteMap = new HashMap<>();
                    }
                    if (fragmentHeatReadMap == null) {
                        fragmentHeatReadMap = new HashMap<>();
                    }
//                logger.error("fragmentHeatReadMap = {}", fragmentHeatReadMap);
//        logger.error("start to load fragments points");
                    Map<FragmentMeta, Long> fragmentMetaPointsMap = metaManager.loadFragmentPoints();
                    logger.error("fragmentMetaPointsMap = {}", fragmentMetaPointsMap);
//        logger.error("start to load fragment of each node");
                    Map<Long, List<FragmentMeta>> fragmentOfEachNode = loadFragmentOfEachNode(
                        fragmentHeatWriteMap, fragmentHeatReadMap);

                    long totalHeats = 0;
                    long maxHeat = 0;
                    long minHeat = Long.MAX_VALUE;
//        logger.error("start to print all fragments of each node");
                    Map<Long, Double> nodeLatencyMap = new HashMap<>();
                    for (Entry<Long, List<FragmentMeta>> fragmentOfEachNodeEntry : fragmentOfEachNode
                        .entrySet()) {
                        long heat = 0;
                        long requests = 0;
                        List<FragmentMeta> fragmentMetas = fragmentOfEachNodeEntry.getValue();
                        for (FragmentMeta fragmentMeta : fragmentMetas) {
                            logger.error("fragment: {}", fragmentMeta.toString());
                            logger.error("fragment heat read: = {}", fragmentHeatReadMap.getOrDefault(fragmentMeta, 0L));
                            logger.error("fragment requests read: = {}", readRequestsMap.getOrDefault(fragmentMeta, 0L));
                            logger.error("fragment heat write: = {}", fragmentHeatWriteMap.getOrDefault(fragmentMeta, 0L));
                            logger.error("fragment requests write: = {}", writeRequestsMap.getOrDefault(fragmentMeta, 0L));
                            heat += fragmentHeatWriteMap.getOrDefault(fragmentMeta, 0L);
                            heat += fragmentHeatReadMap.getOrDefault(fragmentMeta, 0L);
                            requests += writeRequestsMap.getOrDefault(fragmentMeta, 0L);
                            requests += readRequestsMap.getOrDefault(fragmentMeta, 0L);
                        }
                        logger.error("heat of node {} : {}", fragmentOfEachNodeEntry.getKey(), heat);
                        logger.error("requests of node {} : {}", fragmentOfEachNodeEntry.getKey(), requests);
                        if (requests != 0) {
                            nodeLatencyMap.put(fragmentOfEachNodeEntry.getKey(), heat * 1.0 / requests);
                        } else {
                            nodeLatencyMap.put(fragmentOfEachNodeEntry.getKey(), 0.0);
                        }

                        totalHeats += heat;
                        maxHeat = Math.max(maxHeat, heat);
                        minHeat = Math.min(minHeat, heat);
                    }
//        logger.error("end print all fragments of each node");
                    double averageHeats = totalHeats * 1.0 / fragmentOfEachNode.size();
                    if (isAdjustByAverageLatency) {
                        updateHeatByAverageLatency(nodeLatencyMap, fragmentOfEachNode, fragmentHeatWriteMap, fragmentHeatReadMap);
                        totalHeats = 0;
                        maxHeat = 0;
                        minHeat = Long.MAX_VALUE;
                        for (Entry<Long, List<FragmentMeta>> fragmentOfEachNodeEntry : fragmentOfEachNode
                            .entrySet()) {
                            long heat = 0;
                            List<FragmentMeta> fragmentMetas = fragmentOfEachNodeEntry.getValue();
                            for (FragmentMeta fragmentMeta : fragmentMetas) {
                                heat += fragmentHeatWriteMap.getOrDefault(fragmentMeta, 0L);
                                heat += fragmentHeatReadMap.getOrDefault(fragmentMeta, 0L);
                            }
                            logger.error("updated heat of node {} : {}", fragmentOfEachNodeEntry.getKey(), heat);

                            totalHeats += heat;
                            maxHeat = Math.max(maxHeat, heat);
                            minHeat = Math.min(minHeat, heat);
                        }
                        averageHeats = totalHeats * 1.0 / fragmentOfEachNode.size();
                    }

                    if (ConfigDescriptor.getInstance().getConfig().isEnableDynamicMigration()) {
                        if (((1 - unbalanceThreshold) * averageHeats >= minHeat
                            || (1 + unbalanceThreshold) * averageHeats <= maxHeat)) {
                            //发起负载均衡
                            policy.executeReshardAndMigration(fragmentMetaPointsMap, fragmentOfEachNode,
                                fragmentHeatWriteMap, fragmentHeatReadMap, new ArrayList<>());
//                            if (DefaultMetaManager.getInstance().executeReshard()) {
//                                logger.error("start to execute reshard");
//                                //发起负载均衡
//                                policy.executeReshardAndMigration(fragmentMetaPointsMap, fragmentOfEachNode,
//                                    fragmentHeatWriteMap, fragmentHeatReadMap, new ArrayList<>());
//                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("monitor manager error ", e);
            } finally {
//                DefaultMetaManager.getInstance().doneReshard();
            }
        }
    }

    // 根据平均延迟调整各个节点的负载值
    private void updateHeatByAverageLatency(Map<Long, Double> nodeLatencyMap,
                                            Map<Long, List<FragmentMeta>> fragmentOfEachNode,
                                            Map<FragmentMeta, Long> fragmentHeatWriteMap,
                                            Map<FragmentMeta, Long> fragmentHeatReadMap) {
        double maxLatency = 0;
        for (Double latency : nodeLatencyMap.values()) {
            maxLatency = Math.max(maxLatency, latency);
        }

        for (Entry<Long, List<FragmentMeta>> fragmentOfEachNodeEntry : fragmentOfEachNode.entrySet()) {
            long nodeId = fragmentOfEachNodeEntry.getKey();
            double latency = nodeLatencyMap.get(nodeId);
            double adjustRatio = 1.0;
            if (maxLatency != 0) {
                adjustRatio = latency / maxLatency;
            }
            List<FragmentMeta> fragmentMetas = fragmentOfEachNodeEntry.getValue();
            for (FragmentMeta fragmentMeta : fragmentMetas) {
                long writeHeat = fragmentHeatWriteMap.getOrDefault(fragmentMeta, 0L);
                if (writeHeat > 0) {
                    writeHeat = (long) (writeHeat * adjustRatio);
                    fragmentHeatWriteMap.put(fragmentMeta, writeHeat);
                }
                long readHeat = fragmentHeatReadMap.getOrDefault(fragmentMeta, 0L);
                if (readHeat > 0) {
                    readHeat = (long) (readHeat * adjustRatio);
                    fragmentHeatReadMap.put(fragmentMeta, readHeat);
                }
            }
        }
    }

    private Map<FragmentMeta, Long> filterAndGetHeatMap(List<Pair<FragmentMeta, Long>> soredCostList) {
//        if (soredCostList.size() >= 10) {
//            double removeHighPercent = 0.95; // 保留大于等于P99的数据
//            double removeHighHeatTimes = 10; // 保留小于10倍的平均延迟的数据
//            double removeLowPercent = 0.05; // 保留大于等于P1的数据
//            double removeLowHeatTimes = 0.1; // 保留大于0.1倍的平均延迟的数据
//
//            long totalHeat = 0L;
//            List<Long> heatList = new ArrayList<>();
//            Iterator<Pair<FragmentMeta, Long>> soredCostIterator = soredCostList.iterator();
//            while (soredCostIterator.hasNext()) {
//                long latency = soredCostIterator.next().getV();
//                if (latency == 0) {
//                    soredCostIterator.remove();
//                }
//                totalHeat += latency;
//                heatList.add(latency);
//            }
//
//            double averageLatency = totalHeat * 1.0 / soredCostList.size();
//            double highPercentLatency = soredCostList.get((int) Math.floor(soredCostList.size() * removeHighPercent)).getV();
//            double lowPercentLatency = soredCostList.get((int) Math.ceil(soredCostList.size() * removeLowPercent)).getV();
//            soredCostIterator = soredCostList.iterator();
//            while (soredCostIterator.hasNext()) {
//                long latency = soredCostIterator.next().getV();
//                if (latency < lowPercentLatency || latency < averageLatency * removeLowHeatTimes) {
//                    soredCostIterator.remove();
//                } else if (latency > highPercentLatency || latency > averageLatency * removeHighHeatTimes) {
//                    soredCostIterator.remove();
//                }
//            }
//        }

        Map<FragmentMeta, Long> result = new HashMap<>();
        for (Pair<FragmentMeta, Long> pair : soredCostList) {
            long heat = result.getOrDefault(pair.getK(), 0L);
            result.put(pair.getK(), heat + pair.getV());
        }
        return result;
    }

    private Map<Long, List<FragmentMeta>> loadFragmentOfEachNodeWithColdFragment(
        Map<FragmentMeta, Long> fragmentHeatWriteMap, Map<FragmentMeta, Long> fragmentHeatReadMap, List<Long> toScaleInNodes) {
        for (long nodeId : toScaleInNodes) {
            List<FragmentMeta> allFragmentMetas = metaManager.getAllFragmentsByStorageEngineId(nodeId);
            for (FragmentMeta fragmentMeta : allFragmentMetas) {
                if (!fragmentHeatReadMap.containsKey(fragmentMeta)) {
                    fragmentHeatReadMap.put(fragmentMeta, 1L);
                }
                if (!fragmentHeatWriteMap.containsKey(fragmentMeta)) {
                    fragmentHeatWriteMap.put(fragmentMeta, 1L);
                }
            }
        }

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

        List<StorageEngineMeta> storageEngineMetas = metaManager.getStorageEngineList();
        Set<Long> storageIds = result.keySet();
        for (StorageEngineMeta storageEngineMeta : storageEngineMetas) {
            if (!storageIds.contains(storageEngineMeta.getId())) {
                result.put(storageEngineMeta.getId(), new ArrayList<>());
            }
        }
        return result;
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

        List<StorageEngineMeta> storageEngineMetas = metaManager.getStorageEngineList();
        Set<Long> storageIds = result.keySet();
        for (StorageEngineMeta storageEngineMeta : storageEngineMetas) {
            if (!storageIds.contains(storageEngineMeta.getId())) {
                result.put(storageEngineMeta.getId(), new ArrayList<>());
            }
        }
        return result;
    }
}