package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;

import java.util.ArrayList;
import java.util.List;

public class Migration extends AbstractUnaryOperator {

    private final FragmentMeta fragmentMeta;
    private final List<StorageUnitMeta> targetReplicaStorageUnitMetaList;
    private final boolean isChangeStorageUnit;

    public Migration(GlobalSource source, FragmentMeta fragmentMeta,
                     StorageUnitMeta storageUnitMeta) {
        super(OperatorType.Migration, source);
        this.fragmentMeta = fragmentMeta;
        List<StorageUnitMeta> storageUnitMetaList = new ArrayList<>();
        storageUnitMetaList.add(storageUnitMeta);
        this.targetReplicaStorageUnitMetaList = storageUnitMetaList;
        this.isChangeStorageUnit = true;
    }

    public Migration(GlobalSource source, FragmentMeta fragmentMeta,
                     List<StorageUnitMeta> targetReplicaStorageUnitMetaList, boolean isChangeStorageUnit) {
        super(OperatorType.Migration, source);
        this.fragmentMeta = fragmentMeta;
        this.targetReplicaStorageUnitMetaList = targetReplicaStorageUnitMetaList;
        this.isChangeStorageUnit = isChangeStorageUnit;
    }

    public FragmentMeta getFragmentMeta() {
        return fragmentMeta;
    }

    public List<StorageUnitMeta> getTargetReplicaStorageUnitMetaList() {
        return targetReplicaStorageUnitMetaList;
    }

    public boolean isChangeStorageUnit() {
        return isChangeStorageUnit;
    }

    @Override
    public Operator copy() {
        return new Migration((GlobalSource) getSource().copy(), fragmentMeta, targetReplicaStorageUnitMetaList, isChangeStorageUnit);
    }

    @Override
    public String getInfo() {
        return "";
    }
}
