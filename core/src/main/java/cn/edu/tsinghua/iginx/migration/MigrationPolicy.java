package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Migration;
import cn.edu.tsinghua.iginx.engine.shared.operator.ShowTimeSeries;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.cache.DefaultMetaCache;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.migration.recover.MigrationExecuteTask;
import cn.edu.tsinghua.iginx.migration.recover.MigrationExecuteType;
import cn.edu.tsinghua.iginx.migration.recover.MigrationLogger;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;

public abstract class MigrationPolicy {

    protected ExecutorService executor;

    protected static final Config config = ConfigDescriptor.getInstance().getConfig();

    private Logger logger;

    private final IPolicy policy = PolicyManager.getInstance()
        .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());
    private final int maxReshardFragmentsNum = config.getMaxReshardFragmentsNum();
    private static final double maxTimeseriesLoadBalanceThreshold = ConfigDescriptor.getInstance()
        .getConfig().getMaxTimeseriesLoadBalanceThreshold();

    private SortedSet<String> allTimeseriesCache = null;

    private MigrationLogger migrationLogger;

    private final static PhysicalEngine physicalEngine = PhysicalEngineImpl.getInstance();

    public MigrationPolicy(Logger logger) {
        this.logger = logger;
    }

    public void setMigrationLogger(MigrationLogger migrationLogger) {
        this.migrationLogger = migrationLogger;
    }

    public abstract void migrate(List<MigrationTask> migrationTasks,
                                 Map<Long, List<FragmentMeta>> nodeFragmentMap,
                                 Map<FragmentMeta, Long> fragmentWriteLoadMap, Map<FragmentMeta, Long> fragmentReadLoadMap);

    /**
     * 可定制化副本
     */
    public void reshardByCustomizableReplica(FragmentMeta fragmentMeta,
                                             Map<String, Long> timeseriesLoadMap, Set<String> overLoadTimeseries, long totalLoad,
                                             long points, Map<Long, Long> storageHeat) {
        if (config.isEnableCustomizableReplica()) {
            try {
                if (!DefaultMetaManager.getInstance().getCustomizableReplicaFragmentList(fragmentMeta).isEmpty()) {
                    logger.error("customizable replica fragment cannot be reshard");
                    return;
                } else {
                    logger.error("customizableReplicaFragmentMetaList = {}", DefaultMetaCache.getInstance().customizableReplicaFragmentMetaList);
                }
                migrationLogger.logMigrationExecuteTaskStart(
                    new MigrationExecuteTask(fragmentMeta, fragmentMeta.getMasterStorageUnitId(), 0L, 0L,
                        MigrationExecuteType.RESHARD_TIME_SERIES));

                logger.error("start to reshard by customizable replica, fragment = {}", fragmentMeta);
                // 首先将分片结束
                if (fragmentMeta.getTimeInterval().getEndTime() == Long.MAX_VALUE) {
                    List<Long> storageEngineList = new ArrayList<>();
                    storageEngineList.add(fragmentMeta.getMasterStorageUnit().getStorageEngineId());
                    Pair<FragmentMeta, StorageUnitMeta> fragmentMetaStorageUnitMetaPair = policy
                        .generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(
                            fragmentMeta.getTsInterval().getStartTimeSeries(), fragmentMeta.getTsInterval().getEndTimeSeries(),
                            DefaultMetaManager.getInstance().getMaxActiveEndTime(), Long.MAX_VALUE,
                            storageEngineList);
                    DefaultMetaManager.getInstance()
                        .splitFragmentAndStorageUnit(fragmentMetaStorageUnitMetaPair.getV(),
                            fragmentMetaStorageUnitMetaPair.getK(), fragmentMeta);
                }

                // 再开始进行可定制化副本拷贝
                List<String> timeseries = new ArrayList<>(timeseriesLoadMap.keySet());
                timeseries.sort(String::compareTo);
                String currStartTimeseries = fragmentMeta.getTsInterval().getStartTimeSeries();
                long currLoad = 0L;
                String endTimeseries = fragmentMeta.getTsInterval().getEndTimeSeries();
                long startTime = fragmentMeta.getTimeInterval().getStartTime();
                long endTime = fragmentMeta.getTimeInterval().getEndTime();
                StorageUnitMeta storageUnitMeta = fragmentMeta.getMasterStorageUnit();
                List<FragmentMeta> fakedFragmentMetas = new ArrayList<>();
                List<Long> fakedFragmentMetaLoads = new ArrayList<>();
                // 按超负载序列进行分片
                // 单序列情况需要单独考虑
                if (timeseries.size() == 1 || timeseries.isEmpty()) {
                    Set<String> pathRegexSet = new HashSet<>();
                    ShowTimeSeries showTimeSeries = new ShowTimeSeries(new GlobalSource(),
                        pathRegexSet, null, Integer.MAX_VALUE, 0);
                    RowStream rowStream = physicalEngine.execute(showTimeSeries);
                    List<String> pathList = new ArrayList<>();
                    while (rowStream.hasNext()) {
                        Row row = rowStream.next();
                        String timeSeries = new String((byte[]) row.getValue(0));
                        if (timeSeries.contains("{") && timeSeries.contains("}")) {
                            timeSeries = timeSeries.split("\\{")[0];
                        }
                        if (fragmentMeta.getTsInterval().isContain(timeSeries)) {
                            pathList.add(timeSeries);
                        }
                    }
                    pathList.sort(String::compareTo);
                    if (timeseries.size() == 1) {
                        String targetTimeseries = timeseries.get(0);
                        for (int i = 0; i < pathList.size(); i++) {
                            if (pathList.get(i).equals(targetTimeseries)) {
                                if (i > 0 && i < pathList.size() - 1) {
                                    // 前置分片
                                    fakedFragmentMetas.add(new FragmentMeta(currStartTimeseries, pathList.get(i - 1), startTime,
                                        endTime, storageUnitMeta));
                                    fakedFragmentMetaLoads.add(0L);
                                    // 高负载序列分片
                                    fakedFragmentMetas.add(new FragmentMeta(targetTimeseries, pathList.get(i + 1), startTime,
                                        endTime, storageUnitMeta));
                                    fakedFragmentMetaLoads.add(timeseriesLoadMap.get(targetTimeseries));
                                    // 后置分片
                                    fakedFragmentMetas.add(new FragmentMeta(pathList.get(i + 1), endTimeseries, startTime,
                                        endTime, storageUnitMeta));
                                    fakedFragmentMetaLoads.add(0L);
                                } else if (i == 0) {
                                    // 高负载序列分片
                                    fakedFragmentMetas.add(new FragmentMeta(targetTimeseries, pathList.get(1), startTime,
                                        endTime, storageUnitMeta));
                                    fakedFragmentMetaLoads.add(timeseriesLoadMap.get(targetTimeseries));
                                    // 后置分片
                                    fakedFragmentMetas.add(new FragmentMeta(pathList.get(1), endTimeseries, startTime,
                                        endTime, storageUnitMeta));
                                    fakedFragmentMetaLoads.add(0L);
                                } else if (i == pathList.size() - 1) {
                                    // 前置分片
                                    fakedFragmentMetas.add(new FragmentMeta(currStartTimeseries, targetTimeseries, startTime,
                                        endTime, storageUnitMeta));
                                    fakedFragmentMetaLoads.add(0L);
                                    // 高负载序列分片
                                    fakedFragmentMetas.add(new FragmentMeta(targetTimeseries, endTimeseries, startTime,
                                        endTime, storageUnitMeta));
                                    fakedFragmentMetaLoads.add(timeseriesLoadMap.get(targetTimeseries));
                                }
                                break;
                            }
                        }
                    } else {
                        // 前置分片
                        fakedFragmentMetas.add(new FragmentMeta(currStartTimeseries, pathList.get(pathList.size() - 1), startTime,
                            endTime, storageUnitMeta));
                        fakedFragmentMetaLoads.add(0L);
                        // 高负载无效访问序列分片
                        fakedFragmentMetas.add(new FragmentMeta(pathList.get(pathList.size() - 1), endTimeseries, startTime,
                            endTime, storageUnitMeta));
                        // 随便加一些负载
                        totalLoad = 100000L;
                        fakedFragmentMetaLoads.add(100000L);
                        logger.error("one timeseries, fakedFragmentMetas = {}", fakedFragmentMetas);
                    }
                } else {
                    for (int i = 0; i < timeseries.size(); i++) {
                        if (overLoadTimeseries.contains(timeseries.get(i))) {
                            if (!currStartTimeseries.equals(timeseries.get(i))) {
                                fakedFragmentMetas.add(new FragmentMeta(currStartTimeseries, timeseries.get(i), startTime,
                                    endTime, storageUnitMeta));
                                fakedFragmentMetaLoads.add(currLoad);
                                currLoad = 0;
                            }
                            if (i != (timeseries.size() - 1)) {
                                fakedFragmentMetas
                                    .add(new FragmentMeta(timeseries.get(i), timeseries.get(i + 1), startTime,
                                        endTime, storageUnitMeta));
                                fakedFragmentMetaLoads.add(timeseriesLoadMap.get(timeseries.get(i)));
                                currStartTimeseries = timeseries.get(i + 1);
                            } else {
                                currStartTimeseries = timeseries.get(i);
                                currLoad = timeseriesLoadMap.get(timeseries.get(i));
                            }
                        }
                        currLoad += timeseriesLoadMap.get(timeseries.get(i));
                    }
                    fakedFragmentMetas.add(new FragmentMeta(currStartTimeseries, endTimeseries, startTime,
                        endTime, storageUnitMeta));
                    fakedFragmentMetaLoads.add(currLoad);
                }

                // 模拟进行时间序列分片
                while (fakedFragmentMetas.size() > maxReshardFragmentsNum) {
                    double currAverageLoad = totalLoad * 1.0 / fakedFragmentMetaLoads.size();
                    boolean canMergeFragments = false;
                    for (int i = 0; i < fakedFragmentMetaLoads.size(); i++) {
                        FragmentMeta currFragmentMeta = fakedFragmentMetas.get(i);
                        // 合并时间序列分片
                        if (fakedFragmentMetaLoads.get(i) <= currAverageLoad * (1
                            + maxTimeseriesLoadBalanceThreshold) && currFragmentMeta.getTsInterval()
                            .getStartTimeSeries().equals(currFragmentMeta.getTsInterval().getEndTimeSeries())) {

                            // 与他最近的负载最低的时间分区进行合并
                            if (i == (fakedFragmentMetaLoads.size() - 1)
                                || fakedFragmentMetaLoads.get(i + 1) > fakedFragmentMetaLoads.get(i - 1)) {
                                FragmentMeta toMergeFragmentMeta = fakedFragmentMetas.get(i - 1);
                                toMergeFragmentMeta.getTsInterval().setEndTimeSeries(
                                    fakedFragmentMetas.get(i).getTsInterval().getEndTimeSeries());
                                fakedFragmentMetas.remove(i);
                                fakedFragmentMetaLoads
                                    .set(i - 1, fakedFragmentMetaLoads.get(i - 1) + fakedFragmentMetaLoads.get(i));
                                fakedFragmentMetaLoads.remove(i);
                            } else if (fakedFragmentMetaLoads.get(i + 1) <= fakedFragmentMetaLoads.get(i - 1)) {
                                FragmentMeta toMergeFragmentMeta = fakedFragmentMetas.get(i);
                                toMergeFragmentMeta.getTsInterval().setEndTimeSeries(
                                    fakedFragmentMetas.get(i + 1).getTsInterval().getEndTimeSeries());
                                fakedFragmentMetas.remove(i + 1);
                                fakedFragmentMetaLoads
                                    .set(i, fakedFragmentMetaLoads.get(i) + fakedFragmentMetaLoads.get(i + 1));
                                fakedFragmentMetaLoads.remove(i + 1);
                            }

                            // 需要合并
                            canMergeFragments = true;
                        }
                    }
                    // 合并最小分片
                    if (!canMergeFragments) {
                        long maxTwoFragmentLoads = 0L;
                        int startIndex = 0;
                        for (int i = 0; i < fakedFragmentMetaLoads.size(); i++) {
                            if (i < fakedFragmentMetaLoads.size() - 1) {
                                long currTwoFragmentLoad =
                                    fakedFragmentMetaLoads.get(i) + fakedFragmentMetaLoads.get(i + 1);
                                if (currTwoFragmentLoad > maxTwoFragmentLoads) {
                                    maxTwoFragmentLoads = currTwoFragmentLoad;
                                    startIndex = i;
                                }
                            }
                        }
                        FragmentMeta toMergeFragmentMeta = fakedFragmentMetas.get(startIndex);
                        toMergeFragmentMeta.getTsInterval().setEndTimeSeries(
                            fakedFragmentMetas.get(startIndex + 1).getTsInterval().getEndTimeSeries());
                        fakedFragmentMetas.remove(startIndex + 1);
                        fakedFragmentMetaLoads.set(startIndex,
                            fakedFragmentMetaLoads.get(startIndex) + fakedFragmentMetaLoads.get(startIndex + 1));
                        fakedFragmentMetaLoads.remove(startIndex + 1);
                    }
                }

                // 给每个节点负载做排序以方便后续迁移
                // 去掉本身节点
                storageHeat.remove(fragmentMeta.getMasterStorageUnit().getStorageEngineId());
                List<Entry<Long, Long>> storageHeatEntryList = new ArrayList<>(storageHeat.entrySet());
                storageHeatEntryList.sort(Entry.comparingByValue());
                logger.error("customizable replica reshard fragments = {}", fakedFragmentMetas);
                logger.error("fakedFragmentMetaLoads = {}", fakedFragmentMetaLoads);

                // 开始实际切分片
                if (fakedFragmentMetas.size() == 1) {
                    // 如果只有一个分片，则必然要拷贝这个分片
                    FragmentMeta targetFragmentMeta = fakedFragmentMetas.get(0);
                    int replicas = storageHeatEntryList.size();
                    makeReplicaOfTargetFragment(replicas, targetFragmentMeta, storageHeatEntryList);
                } else {
                    double currAverageLoad = totalLoad * 1.0 / fakedFragmentMetaLoads.size();
                    boolean isReplica = false;
                    for (int i = 0; i < fakedFragmentMetas.size(); i++) {
                        FragmentMeta targetFragmentMeta = fakedFragmentMetas.get(i);
                        if (i == 0) {
                            DefaultMetaManager.getInstance().endFragmentByTimeSeriesInterval(fragmentMeta,
                                targetFragmentMeta.getTsInterval().getEndTimeSeries());
                            DefaultMetaManager.getInstance().updateFragmentPoints(targetFragmentMeta, points / fakedFragmentMetas.size());
                        } else {
                            FragmentMeta newFragment = new FragmentMeta(
                                targetFragmentMeta.getTsInterval().getStartTimeSeries(),
                                targetFragmentMeta.getTsInterval().getEndTimeSeries(),
                                fragmentMeta.getTimeInterval().getStartTime(),
                                fragmentMeta.getTimeInterval().getEndTime(), fragmentMeta.getMasterStorageUnit());
                            DefaultMetaManager.getInstance().addFragment(newFragment);
                            DefaultMetaManager.getInstance().updateFragmentPoints(newFragment, points / fakedFragmentMetas.size());
                        }
                        // 开始拷贝副本，有一个先决条件，时间分区必须为闭区间，即只有查询请求，在之后不会被再写入数据，也不会被拆分读写
                        if (fakedFragmentMetaLoads.get(i) >= currAverageLoad * (1
                            + maxTimeseriesLoadBalanceThreshold)) {
                            isReplica = true;
                            // 最多每个节点一个副本
                            int replicas = Math.min((int) (fakedFragmentMetaLoads.get(i) / currAverageLoad), storageHeatEntryList.size());
                            makeReplicaOfTargetFragment(replicas, targetFragmentMeta, storageHeatEntryList);
                        }
                    }
                    // 如果一个副本都没拷贝，则拷贝负载最高的一个
                    logger.error("storageHeatEntryList = {}", storageHeatEntryList);
                    if (!isReplica) {
                        FragmentMeta targetFragmentMeta = null;
                        long maxHeat = 0;
                        for (int i = 0; i < fakedFragmentMetas.size(); i++) {
                            if (fakedFragmentMetaLoads.get(i) > maxHeat) {
                                maxHeat = fakedFragmentMetaLoads.get(i);
                                targetFragmentMeta = fakedFragmentMetas.get(i);
                            }
                        }
                        logger.error("maxtargetFragmentMeta = {}", targetFragmentMeta);
                        logger.error("maxHeat = {}", maxHeat);
                        if (targetFragmentMeta != null) {
                            int replicas = storageHeatEntryList.size();
                            makeReplicaOfTargetFragment(replicas, targetFragmentMeta, storageHeatEntryList);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("execute replica fragment error ", e);
            } finally {
                migrationLogger.logMigrationExecuteTaskEnd();
            }
            logger.error("end execute replica fragment");
        }
    }

    private void makeReplicaOfTargetFragment(int replicas, FragmentMeta targetFragmentMeta, List<Entry<Long, Long>> storageHeatEntryList) throws PhysicalException, MetaStorageException {
        List<FragmentMeta> replicaFragmentMetas = new ArrayList<>();
        replicaFragmentMetas.add(targetFragmentMeta);
        List<StorageUnitMeta> targetReplicaStorageUnitMetaList = new ArrayList<>();

        // 热度信息位空，说明是单序列情况，直接全部拷贝
        if (storageHeatEntryList.isEmpty()) {
            List<StorageEngineMeta> storageEngineMetas = DefaultMetaManager.getInstance().getStorageEngineList();
            for (StorageEngineMeta storageEngineMeta : storageEngineMetas) {
                // 本机id后面统一拷贝
                if (storageEngineMeta.getId() != targetFragmentMeta.getMasterStorageUnit().getStorageEngineId()) {
                    StorageUnitMeta newStorageUnitMeta = DefaultMetaManager.getInstance()
                        .generateNewStorageUnitMetaByFragment(targetFragmentMeta, storageEngineMeta.getId());
                    targetReplicaStorageUnitMetaList.add(newStorageUnitMeta);
                    FragmentMeta newFragment = new FragmentMeta(
                        targetFragmentMeta.getTsInterval().getStartTimeSeries(),
                        targetFragmentMeta.getTsInterval().getEndTimeSeries(),
                        targetFragmentMeta.getTimeInterval().getStartTime(),
                        targetFragmentMeta.getTimeInterval().getEndTime(), newStorageUnitMeta);
                    replicaFragmentMetas.add(newFragment);
                }
            }
        } else {
            for (int num = 0; num < replicas; num++) {
                long targetStorageId = storageHeatEntryList.get(num).getKey();
                StorageUnitMeta newStorageUnitMeta = DefaultMetaManager.getInstance()
                    .generateNewStorageUnitMetaByFragment(targetFragmentMeta, targetStorageId);
                targetReplicaStorageUnitMetaList.add(newStorageUnitMeta);
                FragmentMeta newFragment = new FragmentMeta(
                    targetFragmentMeta.getTsInterval().getStartTimeSeries(),
                    targetFragmentMeta.getTsInterval().getEndTimeSeries(),
                    targetFragmentMeta.getTimeInterval().getStartTime(),
                    targetFragmentMeta.getTimeInterval().getEndTime(), newStorageUnitMeta);
                replicaFragmentMetas.add(newFragment);
            }
        }
        // 本地新建 du 并拷贝数据
//        StorageUnitMeta newLocalStorageUnitMeta = DefaultMetaManager.getInstance()
//            .generateNewStorageUnitMetaByFragment(targetFragmentMeta, targetFragmentMeta.getMasterStorageUnit().getStorageEngineId());
//        targetReplicaStorageUnitMetaList.add(newLocalStorageUnitMeta);
//        FragmentMeta newLocalFragment = new FragmentMeta(
//            targetFragmentMeta.getTsInterval().getStartTimeSeries(),
//            targetFragmentMeta.getTsInterval().getEndTimeSeries(),
//            targetFragmentMeta.getTimeInterval().getStartTime(),
//            targetFragmentMeta.getTimeInterval().getEndTime(), newLocalStorageUnitMeta);
//        replicaFragmentMetas.add(newLocalFragment);

        logger.error("targetReplicaStorageUnitMetaList = {}", targetReplicaStorageUnitMetaList);
        // 开始拷贝数据
        Migration migration = new Migration(new GlobalSource(), targetFragmentMeta, targetReplicaStorageUnitMetaList, false);
        physicalEngine.execute(migration);
        logger.error("complete execute migration");
        DefaultMetaManager.getInstance().addCustomizableReplicaFragmentMeta(targetFragmentMeta, replicaFragmentMetas);
        logger.error("complete addCustomizableReplicaFragmentMeta");
    }

    /**
     * 在时间序列层面将分片在同一个du下分为两块（未知时间序列, 写入场景）
     */
    public void reshardWriteByTimeseries(FragmentMeta fragmentMeta, long points)
        throws PhysicalException {
        // 分区不存在直接返回
//    if (!DefaultMetaManager.getInstance()
//        .checkFragmentExistenceByTimeInterval(fragmentMeta.getTsInterval())) {
//      return;
//    }
        try {
            migrationLogger.logMigrationExecuteTaskStart(
                new MigrationExecuteTask(fragmentMeta, fragmentMeta.getMasterStorageUnitId(), 0L, 0L,
                    MigrationExecuteType.RESHARD_TIME_SERIES));

            SortedSet<String> pathSet = getAllTimeseriesOfFragment(fragmentMeta);
            if (pathSet.size() <= 1) {
                return;
            }
            String middleTimeseries = new ArrayList<>(pathSet).get(pathSet.size() / 2);
            TimeSeriesInterval sourceTsInterval = new TimeSeriesInterval(
                fragmentMeta.getTsInterval().getStartTimeSeries(),
                fragmentMeta.getTsInterval().getEndTimeSeries());
            FragmentMeta newFragment = new FragmentMeta(middleTimeseries,
                sourceTsInterval.getEndTimeSeries(),
                fragmentMeta.getTimeInterval().getStartTime(),
                fragmentMeta.getTimeInterval().getEndTime(), fragmentMeta.getMasterStorageUnit());
            logger.error("timeseries split new fragment=" + newFragment.toString());
            DefaultMetaManager.getInstance().addFragment(newFragment);
            DefaultMetaManager.getInstance().updateFragmentPoints(newFragment, points / 2);
            DefaultMetaManager.getInstance()
                .endFragmentByTimeSeriesInterval(fragmentMeta, middleTimeseries);
            DefaultMetaManager.getInstance().updateFragmentPoints(fragmentMeta, points / 2);
        } finally {
            migrationLogger.logMigrationExecuteTaskEnd();
        }
    }

    private SortedSet<String> getAllTimeseriesOfFragment(FragmentMeta fragmentMeta) throws PhysicalException {
        SortedSet<String> pathSet = new TreeSet<>();
        Set<String> pathRegexSet = new HashSet<>();
        if (allTimeseriesCache == null) {
            allTimeseriesCache = new TreeSet<>();
            ShowTimeSeries showTimeSeries = new ShowTimeSeries(new GlobalSource(),
                pathRegexSet, null, Integer.MAX_VALUE, 0);
            RowStream rowStream = physicalEngine.execute(showTimeSeries);
            while (rowStream.hasNext()) {
                Row row = rowStream.next();
                String timeSeries = new String((byte[]) row.getValue(0));
                if (timeSeries.contains("{") && timeSeries.contains("}")) {
                    timeSeries = timeSeries.split("\\{")[0];
                }
                allTimeseriesCache.add(timeSeries);
                if (fragmentMeta.getTsInterval().isContain(timeSeries)) {
                    pathSet.add(timeSeries);
                }
            }
        } else {
            for (String timeSeries : allTimeseriesCache) {
                if (fragmentMeta.getTsInterval().isContain(timeSeries)) {
                    pathSet.add(timeSeries);
                }
            }
        }
        return pathSet;
    }

    /**
     * 在时间序列层面将分片在同一个du下分为两块（已知时间序列）
     */
    public boolean reshardQueryByTimeseries(FragmentMeta fragmentMeta,
                                            Map<String, Long> timeseriesLoadMap, long points) {
        try {
            logger.error("reshard query by timeseries {}", fragmentMeta);
            migrationLogger.logMigrationExecuteTaskStart(
                new MigrationExecuteTask(fragmentMeta, fragmentMeta.getMasterStorageUnitId(), 0L, 0L,
                    MigrationExecuteType.RESHARD_TIME_SERIES));
            long totalLoad = 0L;
            for (Entry<String, Long> timeseriesLoadEntry : timeseriesLoadMap.entrySet()) {
                totalLoad += timeseriesLoadEntry.getValue();
            }
            String middleTimeseries = null;
            long currLoad = 0L;
            for (Entry<String, Long> timeseriesLoadEntry : timeseriesLoadMap.entrySet()) {
                currLoad += timeseriesLoadEntry.getValue();
                if (currLoad >= totalLoad / 2) {
                    middleTimeseries = timeseriesLoadEntry.getKey();
                    break;
                }
            }

            // 如果只有1或2条序列，则无法切分，考虑后面使用可定制化副本
            if (middleTimeseries == null) {
                return false;
            }
            if (fragmentMeta.getTsInterval().getStartTimeSeries() != null && fragmentMeta.getTsInterval().getStartTimeSeries().equals(middleTimeseries)) {
                return false;
            }
            if (fragmentMeta.getTsInterval().getEndTimeSeries() != null && fragmentMeta.getTsInterval().getEndTimeSeries().equals(middleTimeseries)) {
                return false;
            }
            logger.error("migrate middle timeseries {}", middleTimeseries);

            TimeSeriesInterval sourceTsInterval = new TimeSeriesInterval(
                fragmentMeta.getTsInterval().getStartTimeSeries(),
                fragmentMeta.getTsInterval().getEndTimeSeries());
            FragmentMeta newFragment = new FragmentMeta(middleTimeseries,
                sourceTsInterval.getEndTimeSeries(),
                fragmentMeta.getTimeInterval().getStartTime(),
                fragmentMeta.getTimeInterval().getEndTime(), fragmentMeta.getMasterStorageUnit());
            DefaultMetaManager.getInstance().addFragment(newFragment);
            DefaultMetaManager.getInstance().updateFragmentPoints(newFragment, points / 2);
            DefaultMetaManager.getInstance()
                .endFragmentByTimeSeriesInterval(fragmentMeta, middleTimeseries);
            DefaultMetaManager.getInstance().updateFragmentPoints(fragmentMeta, points / 2);
            return true;
        } finally {
            migrationLogger.logMigrationExecuteTaskEnd();
        }
    }

    public void interrupt() {
        executor.shutdown();
    }

    protected boolean canExecuteTargetMigrationTask(MigrationTask migrationTask,
                                                    Map<Long, Long> nodeLoadMap) {
//    long currTargetNodeLoad = nodeLoadMap.getOrDefault(migrationTask.getTargetStorageId(), 0L);
//    logger.error("currTargetNodeLoad = {}", currTargetNodeLoad);
//    logger.error("migrationTask.getLoad() = {}", migrationTask.getLoad());
//    logger.error("config.getMaxLoadThreshold() = {}", config.getMaxLoadThreshold());
        return true;
    }

    protected boolean isAllQueueEmpty(List<Queue<MigrationTask>> migrationTaskQueueList) {
        for (Queue<MigrationTask> migrationTaskQueue : migrationTaskQueueList) {
            if (!migrationTaskQueue.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    protected void sortQueueListByFirstItem(List<Queue<MigrationTask>> migrationTaskQueue) {
        migrationTaskQueue
            .sort((o1, o2) -> {
                MigrationTask migrationTask1 = o1.peek();
                MigrationTask migrationTask2 = o2.peek();
                if (migrationTask1 == null || migrationTask2 == null) {
                    return 1;
                } else {
                    return (int) (migrationTask2.getPriorityScore() - migrationTask1.getPriorityScore());
                }
            });
    }

    protected Map<Long, Long> calculateNodeLoadMap(Map<Long, List<FragmentMeta>> nodeFragmentMap,
                                                   Map<FragmentMeta, Long> fragmentWriteLoadMap, Map<FragmentMeta, Long> fragmentReadLoadMap) {
        Map<Long, Long> nodeLoadMap = new HashMap<>();
        for (Entry<Long, List<FragmentMeta>> nodeFragmentEntry : nodeFragmentMap.entrySet()) {
            List<FragmentMeta> fragmentMetas = nodeFragmentEntry.getValue();
            for (FragmentMeta fragmentMeta : fragmentMetas) {
                nodeLoadMap.put(nodeFragmentEntry.getKey(),
                    fragmentWriteLoadMap.getOrDefault(fragmentMeta, 0L) + fragmentReadLoadMap
                        .getOrDefault(fragmentMeta, 0L));
            }
        }
        return nodeLoadMap;
    }

    protected synchronized void executeOneRoundMigration(
        List<Queue<MigrationTask>> migrationTaskQueueList,
        Map<Long, Long> nodeLoadMap) {
        for (Queue<MigrationTask> migrationTaskQueue : migrationTaskQueueList) {
            MigrationTask migrationTask = migrationTaskQueue.peek();
            //根据负载判断是否能进行该任务
            if (migrationTask != null && canExecuteTargetMigrationTask(migrationTask, nodeLoadMap)) {
                migrationTaskQueue.poll();
                this.executor.submit(() -> {
                    this.logger.info("start migration: {}", migrationTask);
                    //异步执行 耗时的操作
                    if (migrationTask.getMigrationType() == MigrationType.QUERY) {
                        // 如果之前没切过分区，需要优先切一下分区
                        if (migrationTask.getFragmentMeta().getTimeInterval().getEndTime() == Long.MAX_VALUE) {
                            if (migrationTask.getFragmentMeta().getTimeInterval().getStartTime() == 0) {
                                this.logger.error("start to reshard query data: {}", migrationTask);
                                FragmentMeta fragmentMeta = reshardFragment(migrationTask.getSourceStorageId(),
                                    migrationTask.getSourceStorageId(),
                                    migrationTask.getFragmentMeta());
                                migrationTask.setFragmentMeta(fragmentMeta);
                            }
                        }
                        this.logger.error("start to migrate data: {}", migrationTask);
                        migrateData(migrationTask.getSourceStorageId(),
                            migrationTask.getTargetStorageId(),
                            migrationTask.getFragmentMeta());
                    } else if (migrationTask.getMigrationType() == MigrationType.WRITE) {
                        this.logger.error("start to migrate write data: {}", migrationTask);
                        reshardFragment(migrationTask.getSourceStorageId(),
                            migrationTask.getTargetStorageId(),
                            migrationTask.getFragmentMeta());
                    } else {
                        this.logger.error("start to migrate whole data: {}", migrationTask);
                        // 写入迁移
                        FragmentMeta fragmentMeta = reshardFragment(migrationTask.getSourceStorageId(),
                            migrationTask.getTargetStorageId(),
                            migrationTask.getFragmentMeta());
                        if (fragmentMeta != null) {
                            migrationTask.setFragmentMeta(fragmentMeta);
                        }
                        // 查询迁移
                        migrateData(migrationTask.getSourceStorageId(),
                            migrationTask.getTargetStorageId(),
                            migrationTask.getFragmentMeta());
                    }
                    this.logger
                        .error("complete one migration task from {} to {} with load: {}, size: {}, type: {}",
                            migrationTask.getSourceStorageId(), migrationTask.getTargetStorageId(),
                            migrationTask.getLoad(), migrationTask.getSize(),
                            migrationTask.getMigrationType());
                    // 执行下一轮判断
                    while (!isAllQueueEmpty(migrationTaskQueueList)) {
                        executeOneRoundMigration(migrationTaskQueueList, nodeLoadMap);
                    }
                });
            }
        }
        sortQueueListByFirstItem(migrationTaskQueueList);
    }

    private synchronized void migrateData(long sourceStorageId, long targetStorageId,
                                          FragmentMeta fragmentMeta) {
        try {
            // 在目标节点创建新du
            StorageUnitMeta storageUnitMeta;
            try {
                storageUnitMeta = DefaultMetaManager.getInstance()
                    .generateNewStorageUnitMetaByFragment(fragmentMeta, targetStorageId);
            } catch (MetaStorageException e) {
                logger.error("cannot create storage unit in target storage engine", e);
                throw new PhysicalException(e);
            }
            migrationLogger.logMigrationExecuteTaskStart(
                new MigrationExecuteTask(fragmentMeta, storageUnitMeta.getId(), sourceStorageId,
                    targetStorageId,
                    MigrationExecuteType.MIGRATION));

            // 开始迁移数据
            Migration migration = new Migration(new GlobalSource(), fragmentMeta, storageUnitMeta);
            physicalEngine.execute(migration);
            // 迁移完开始删除原数据
//            List<String> paths = new ArrayList<>();
//            paths.add(fragmentMeta.getMasterStorageUnitId() + "*");
//            List<TimeRange> timeRanges = new ArrayList<>();
//            timeRanges.add(new TimeRange(fragmentMeta.getTimeInterval().getStartTime(), true,
//                    fragmentMeta.getTimeInterval().getEndTime(), false));
//            Delete delete = new Delete(new FragmentSource(fragmentMeta), timeRanges, paths, null);
//            physicalEngine.execute(delete);
        } catch (Exception e) {
            logger.error("encounter error when migrate data from {} to {} ", sourceStorageId,
                targetStorageId, e);
        } finally {
            migrationLogger.logMigrationExecuteTaskEnd();
        }
    }

    protected synchronized void executeAllWriteMigrationTask(List<MigrationTask> migrationTasks) {
        if (migrationTasks.stream().anyMatch(migrationTask -> migrationTask.getMigrationType() == MigrationType.WRITE)) {
            try {
                long middleTime = DefaultMetaManager.getInstance().getMaxActiveEndTime();
                // 时间维度必须对齐，因此切一个就必须全切
                Map<TimeSeriesRange, FragmentMeta> timeSeriesRangeFragmentMetaMap = DefaultMetaManager.getInstance().getLatestFragmentMap();
                for (Entry<TimeSeriesRange, FragmentMeta> entry : timeSeriesRangeFragmentMetaMap.entrySet()) {
                    TimeSeriesRange timeSeriesRange = entry.getKey();
                    FragmentMeta currFragmentMeta = entry.getValue();
                    Iterator<MigrationTask> migrationTaskIterable = migrationTasks.iterator();
                    boolean isInMigrationTask = false;
                    while (migrationTaskIterable.hasNext()) {
                        MigrationTask migrationTask = migrationTaskIterable.next();
                        TimeSeriesRange tsInterval = migrationTask.getFragmentMeta().getTsInterval();
                        if (checkTimeseriesEqual(timeSeriesRange.getStartTimeSeries(), tsInterval.getStartTimeSeries()) && checkTimeseriesEqual(timeSeriesRange.getEndTimeSeries(), tsInterval.getEndTimeSeries())) {
                            List<Long> storageEngineList = new ArrayList<>();
                            storageEngineList.add(migrationTask.getTargetStorageId());
                            Pair<FragmentMeta, StorageUnitMeta> fragmentMetaStorageUnitMetaPair = policy
                                .generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(
                                    tsInterval.getStartTimeSeries(), tsInterval.getEndTimeSeries(),
                                    middleTime, Long.MAX_VALUE,
                                    storageEngineList);
                            DefaultMetaManager.getInstance()
                                .splitFragmentAndStorageUnit(fragmentMetaStorageUnitMetaPair.getV(),
                                    fragmentMetaStorageUnitMetaPair.getK(), migrationTask.getFragmentMeta());
                            migrationTaskIterable.remove();
                            isInMigrationTask = true;
                            break;
                        }
                    }
                    if (!isInMigrationTask) {
                        // 保留在原节点
                        logger.error("reshard fragment = {}", currFragmentMeta);
                        logger.error("middle time = {}", middleTime);
                        if (middleTime > currFragmentMeta.getTimeInterval().getStartTime()) {
                            DefaultMetaManager.getInstance().endFragmentByTimeInterval(currFragmentMeta, middleTime);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("reshardFragment error ", e);
            } finally {
                migrationLogger.logMigrationExecuteTaskEnd();
            }
        }
    }

    private synchronized FragmentMeta migrateFragment(long sourceStorageId, long targetStorageId,
                                                      FragmentMeta fragmentMeta) {
        try {
            // [startTime, +∞) & (startPath, endPath)
            TimeSeriesRange tsInterval = fragmentMeta.getTsInterval();
            TimeInterval timeInterval = fragmentMeta.getTimeInterval();
            long middleTime = DefaultMetaManager.getInstance().getMaxActiveEndTime();
            FragmentMeta result = null;

            // 排除乱序写入问题
            if (timeInterval.getEndTime() == Long.MAX_VALUE) {
                // 时间维度必须对齐，因此切一个就必须全切
                Map<TimeSeriesRange, FragmentMeta> timeSeriesRangeFragmentMetaMap = DefaultMetaManager.getInstance().getLatestFragmentMap();
                for (Entry<TimeSeriesRange, FragmentMeta> entry : timeSeriesRangeFragmentMetaMap.entrySet()) {
                    TimeSeriesRange timeSeriesRange = entry.getKey();
                    FragmentMeta currFragmentMeta = entry.getValue();
                    if (checkTimeseriesEqual(timeSeriesRange.getStartTimeSeries(), tsInterval.getStartTimeSeries()) && checkTimeseriesEqual(timeSeriesRange.getEndTimeSeries(), tsInterval.getEndTimeSeries())) {
                        if (sourceStorageId != targetStorageId) {
                            if (middleTime > currFragmentMeta.getTimeInterval().getStartTime()) {
                                List<Long> storageEngineList = new ArrayList<>();
                                storageEngineList.add(targetStorageId);
                                Pair<FragmentMeta, StorageUnitMeta> fragmentMetaStorageUnitMetaPair = policy
                                    .generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(
                                        tsInterval.getStartTimeSeries(), tsInterval.getEndTimeSeries(),
                                        middleTime, Long.MAX_VALUE,
                                        storageEngineList);
                                result = DefaultMetaManager.getInstance()
                                    .splitFragmentAndStorageUnit(fragmentMetaStorageUnitMetaPair.getV(),
                                        fragmentMetaStorageUnitMetaPair.getK(), fragmentMeta);
                            }
                        } else {
                            if (middleTime > currFragmentMeta.getTimeInterval().getStartTime()) {
                                result = DefaultMetaManager.getInstance().endFragmentByTimeInterval(currFragmentMeta, middleTime);
                            } else {
                                result = fragmentMeta;
                            }
                        }
                    } else {
                        // 保留在原节点
                        logger.error("reshard fragment = {}", currFragmentMeta);
                        logger.error("middle time = {}", middleTime);
                        if (middleTime > currFragmentMeta.getTimeInterval().getStartTime()) {
                            DefaultMetaManager.getInstance().endFragmentByTimeInterval(currFragmentMeta, middleTime);
                        }
                    }
                }
                return result;
            }
        } catch (Exception e) {
            logger.error("reshardFragment error ", e);
        } finally {
            migrationLogger.logMigrationExecuteTaskEnd();
        }
        return null;
    }

    private synchronized FragmentMeta reshardFragment(long sourceStorageId, long targetStorageId,
                                                      FragmentMeta fragmentMeta) {
        try {
            migrationLogger.logMigrationExecuteTaskStart(
                new MigrationExecuteTask(fragmentMeta, fragmentMeta.getMasterStorageUnitId(),
                    sourceStorageId, targetStorageId,
                    MigrationExecuteType.RESHARD_TIME));
            // [startTime, +∞) & (startPath, endPath)
            TimeSeriesRange tsInterval = fragmentMeta.getTsInterval();
            TimeInterval timeInterval = fragmentMeta.getTimeInterval();
            long middleTime = DefaultMetaManager.getInstance().getMaxActiveEndTime();
            FragmentMeta result = null;

            // 排除乱序写入问题
            if (timeInterval.getEndTime() == Long.MAX_VALUE) {
                // 时间维度必须对齐，因此切一个就必须全切
                Map<TimeSeriesRange, FragmentMeta> timeSeriesRangeFragmentMetaMap = DefaultMetaManager.getInstance().getLatestFragmentMap();
                for (Entry<TimeSeriesRange, FragmentMeta> entry : timeSeriesRangeFragmentMetaMap.entrySet()) {
                    TimeSeriesRange timeSeriesRange = entry.getKey();
                    FragmentMeta currFragmentMeta = entry.getValue();
                    if (checkTimeseriesEqual(timeSeriesRange.getStartTimeSeries(), tsInterval.getStartTimeSeries()) && checkTimeseriesEqual(timeSeriesRange.getEndTimeSeries(), tsInterval.getEndTimeSeries())) {
                        if (sourceStorageId != targetStorageId) {
                            if (middleTime > currFragmentMeta.getTimeInterval().getStartTime()) {
                                List<Long> storageEngineList = new ArrayList<>();
                                storageEngineList.add(targetStorageId);
                                Pair<FragmentMeta, StorageUnitMeta> fragmentMetaStorageUnitMetaPair = policy
                                    .generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(
                                        tsInterval.getStartTimeSeries(), tsInterval.getEndTimeSeries(),
                                        middleTime, Long.MAX_VALUE,
                                        storageEngineList);
                                result = DefaultMetaManager.getInstance()
                                    .splitFragmentAndStorageUnit(fragmentMetaStorageUnitMetaPair.getV(),
                                        fragmentMetaStorageUnitMetaPair.getK(), fragmentMeta);
                            }
                        } else {
                            if (middleTime > currFragmentMeta.getTimeInterval().getStartTime()) {
                                result = DefaultMetaManager.getInstance().endFragmentByTimeInterval(currFragmentMeta, middleTime);
                            } else {
                                result = fragmentMeta;
                            }
                        }
                    } else {
                        // 保留在原节点
                        logger.error("reshard fragment = {}", currFragmentMeta);
                        logger.error("middle time = {}", middleTime);
                        if (middleTime > currFragmentMeta.getTimeInterval().getStartTime()) {
                            DefaultMetaManager.getInstance().endFragmentByTimeInterval(currFragmentMeta, middleTime);
                        }
                    }
                }
                return result;
            }
        } catch (Exception e) {
            logger.error("reshardFragment error ", e);
        } finally {
            migrationLogger.logMigrationExecuteTaskEnd();
        }
        return null;
    }

    private boolean checkTimeseriesEqual(String timeseries1, String timeseries2) {
        try {
            if (timeseries1 == null && timeseries2 == null) {
                return true;
            } else if (timeseries1.equals(timeseries2)) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }
}
