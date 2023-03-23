/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.engine.physical;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.MemoryPhysicalTaskDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizerManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.StoragePhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.engine.physical.task.*;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Migration;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.cache.DefaultMetaCache;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PhysicalEngineImpl implements PhysicalEngine {

    private static final Logger logger = LoggerFactory.getLogger(PhysicalEngineImpl.class);

    private final ExecutorService migrationService = new ThreadPoolExecutor(10, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());

    private static final PhysicalEngineImpl INSTANCE = new PhysicalEngineImpl();

    private final PhysicalOptimizer optimizer;

    private final MemoryPhysicalTaskDispatcher memoryTaskExecutor;

    private final StoragePhysicalTaskExecutor storageTaskExecutor;

    private PhysicalEngineImpl() {
        optimizer = PhysicalOptimizerManager.getInstance().getOptimizer(ConfigDescriptor.getInstance().getConfig().getPhysicalOptimizer());
        memoryTaskExecutor = MemoryPhysicalTaskDispatcher.getInstance();
        storageTaskExecutor = StoragePhysicalTaskExecutor.getInstance();
        storageTaskExecutor.init(memoryTaskExecutor, optimizer.getReplicaDispatcher());
        memoryTaskExecutor.startDispatcher();
    }

    public static PhysicalEngineImpl getInstance() {
        return INSTANCE;
    }

    @Override
    public RowStream execute(Operator root) throws PhysicalException {
        if (OperatorType.isGlobalOperator(root.getType())) { // 全局任务临时兼容逻辑
            // 迁移任务单独处理
            if (root.getType() == OperatorType.Migration) {
                Migration migration = (Migration) root;
                FragmentMeta toMigrateFragment = migration.getFragmentMeta();
                List<StorageUnitMeta> targetReplicaStorageUnitMetaList = migration.getTargetReplicaStorageUnitMetaList();
                List<String> storageUnitIds = new ArrayList<>();
                for (StorageUnitMeta storageUnitMeta : targetReplicaStorageUnitMetaList) {
                    storageUnitIds.add(storageUnitMeta.getId());
                }

                // 查询分区数据
                List<Operator> operators = new ArrayList<>();
                Project project = new Project(new FragmentSource(toMigrateFragment), Collections.singletonList("*"), null);
                operators.add(project);
                StoragePhysicalTask physicalTask = new StoragePhysicalTask(operators);
                physicalTask.setMigration(true);

                RowStream selectRowStream = null;
                while (selectRowStream == null) {
                    if (migration.getFragmentMeta().getMasterStorageUnitId() != null) {
                        storageTaskExecutor.commitWithTargetStorageUnitId(physicalTask, migration.getFragmentMeta().getMasterStorageUnitId());
                    } else {
                        storageTaskExecutor.commit(physicalTask);
                    }

                    TaskExecuteResult selectResult = physicalTask.getResult();
                    selectRowStream = selectResult.getRowStream();
                }

                List<String> selectResultPaths = new ArrayList<>();
                List<DataType> selectResultTypes = new ArrayList<>();
                selectRowStream.getHeader().getFields().forEach(field -> {
                    if (toMigrateFragment.getTsInterval().isContain(field.getName())) {
                        selectResultPaths.add(field.getName());
                        selectResultTypes.add(field.getType());
                    }
                });

                List<Long> timestampList = new ArrayList<>();
                List<ByteBuffer> valuesList = new ArrayList<>();
                List<Bitmap> bitmapList = new ArrayList<>();

                boolean hasTimestamp = selectRowStream.getHeader().hasKey();
                while (selectRowStream.hasNext()) {
                    Row row = selectRowStream.next();
                    Object[] rowValues = row.getValues();
                    valuesList.add(ByteUtils.getRowByteBuffer(rowValues, selectResultTypes));
                    Bitmap bitmap = new Bitmap(rowValues.length);
                    for (int i = 0; i < rowValues.length; i++) {
                        if (rowValues[i] != null) {
                            bitmap.mark(i);
                        }
                    }
                    bitmapList.add(bitmap);
                    if (hasTimestamp) {
                        timestampList.add(row.getKey());
                    }

                    // 按行批量插入数据
                    if (timestampList.size() == ConfigDescriptor.getInstance().getConfig()
                        .getMigrationBatchSize()) {
//                        migrationService.execute(() -> {
//                            try {
//                                long startTime = System.currentTimeMillis();
//                                insertDataByBatch(new ArrayList<>(timestampList), new ArrayList<>(valuesList), new ArrayList<>(bitmapList), toMigrateFragment, selectResultPaths, selectResultTypes, storageUnitIds);
//                                logger.error("insertDataByBatch consumption time : {}", (System.currentTimeMillis() - startTime));
//                            } catch (PhysicalException e) {
//                                logger.error("Migration data failure!", e);
//                            }
//                        });
                        long startTime = System.currentTimeMillis();
                        insertDataByBatch(timestampList, valuesList, bitmapList, toMigrateFragment, selectResultPaths, selectResultTypes, storageUnitIds);
                        timestampList.clear();
                        valuesList.clear();
                        bitmapList.clear();
                    }
                }
                insertDataByBatch(timestampList, valuesList, bitmapList, toMigrateFragment, selectResultPaths, selectResultTypes, storageUnitIds);

                // 设置分片现在所属的du
                if (migration.isChangeStorageUnit()) {
                    toMigrateFragment.setMasterStorageUnit(targetReplicaStorageUnitMetaList.get(0));
                    DefaultMetaManager.getInstance().updateFragmentByTsInterval(toMigrateFragment.getTsInterval(), toMigrateFragment);
                }
                return selectRowStream;
            } else {
                GlobalPhysicalTask task = new GlobalPhysicalTask(root);
                TaskExecuteResult result = storageTaskExecutor.executeGlobalTask(task);
                if (result.getException() != null) {
                    throw result.getException();
                }
                return result.getRowStream();
            }
        }
        PhysicalTask task = optimizer.optimize(root);
        List<StoragePhysicalTask> storageTasks = new ArrayList<>();
        getStorageTasks(storageTasks, task);
        if (ConfigDescriptor.getInstance().getConfig().isEnableCustomizableReplica()) {
            for (StoragePhysicalTask storagePhysicalTask : storageTasks) {
                // 如果是查询请求，可能有可配置化副本，随机发送到任何一个副本上
                if (!storagePhysicalTask.getOperators().isEmpty() && storagePhysicalTask.getOperators().get(0).getType() == OperatorType.Project) {
                    FragmentMeta fragment = storagePhysicalTask.getTargetFragment();
                    if (fragment != null) {
                        List<FragmentMeta> fragmentMetas = DefaultMetaCache.getInstance().getCustomizableReplicaFragmentList(fragment);
                        if (!fragmentMetas.isEmpty()) {
                            int randomIndex = new Random().nextInt(fragmentMetas.size());
                            FragmentMeta fragmentMeta = fragmentMetas.get(randomIndex);
                            storagePhysicalTask.setTargetFragment(fragmentMetas.get(randomIndex));
                            storagePhysicalTask.setStorageUnit(fragmentMeta.getMasterStorageUnitId());
                            storagePhysicalTask.setStorage(fragmentMeta.getMasterStorageUnit().getStorageEngineId());
                        }
                    }
                }
            }
        }
        storageTaskExecutor.commit(storageTasks);
        TaskExecuteResult result = task.getResult();
        if (result.getException() != null) {
            throw result.getException();
        }
        return result.getRowStream();
    }

    private void insertDataByBatch(List<Long> timestampList, List<ByteBuffer> valuesList,
                                   List<Bitmap> bitmapList, FragmentMeta toMigrateFragment,
                                   List<String> selectResultPaths, List<DataType> selectResultTypes, List<String> storageUnitIds)
        throws PhysicalException {
        // 按行批量插入数据
        List<ByteBuffer> bitmapBufferList = new ArrayList<>();
        for (Bitmap bitmap : bitmapList) {
            bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
        }
        Object[] rowValueObjects = ByteUtils.getRowValuesByDataType(valuesList, selectResultTypes, bitmapBufferList);
        for (String storageUnitId : storageUnitIds) {
            List<Bitmap> copiedBitmapList = new ArrayList<>();
            for (Bitmap bitmap : bitmapList) {
                Bitmap copiedBitmap = new Bitmap(bitmap.getSize(), bitmap.getBytes());
                copiedBitmapList.add(copiedBitmap);
            }
            RawData rowData = new RawData(selectResultPaths, Collections.emptyList(), timestampList, rowValueObjects,
                selectResultTypes, copiedBitmapList, RawDataType.NonAlignedRow);
            RowDataView rowDataView = new RowDataView(rowData, 0, selectResultPaths.size(), 0,
                timestampList.size());
            List<Operator> insertOperators = new ArrayList<>();
            Insert insert = new Insert(new FragmentSource(toMigrateFragment), rowDataView);
            insertOperators.add(insert);
            StoragePhysicalTask insertPhysicalTask = new StoragePhysicalTask(insertOperators);
            insertPhysicalTask.setMigration(true);
            storageTaskExecutor.commitWithTargetStorageUnitId(insertPhysicalTask, storageUnitId);
            TaskExecuteResult insertResult = insertPhysicalTask.getResult();
            if (insertResult.getException() != null) {
                throw insertResult.getException();
            }
        }
    }

    private void getStorageTasks(List<StoragePhysicalTask> tasks, PhysicalTask root) {
        if (root == null) {
            return;
        }
        if (root.getType() == TaskType.Storage) {
            tasks.add((StoragePhysicalTask) root);
        } else if (root.getType() == TaskType.BinaryMemory) {
            BinaryMemoryPhysicalTask task = (BinaryMemoryPhysicalTask) root;
            getStorageTasks(tasks, task.getParentTaskA());
            getStorageTasks(tasks, task.getParentTaskB());
        } else if (root.getType() == TaskType.UnaryMemory) {
            UnaryMemoryPhysicalTask task = (UnaryMemoryPhysicalTask) root;
            getStorageTasks(tasks, task.getParentTask());
        } else if (root.getType() == TaskType.MultipleMemory) {
            MultipleMemoryPhysicalTask task = (MultipleMemoryPhysicalTask) root;
            for (PhysicalTask parentTask : task.getParentTasks()) {
                getStorageTasks(tasks, parentTask);
            }
        }
    }

    @Override
    public ConstraintManager getConstraintManager() {
        return optimizer.getConstraintManager();
    }

    @Override
    public StorageManager getStorageManager() {
        return storageTaskExecutor.getStorageManager();
    }
}
