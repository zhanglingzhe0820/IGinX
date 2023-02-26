package cn.edu.tsinghua.iginx.metadata.hook;

import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;

import java.util.List;

public interface CustomizableReplicaFragmentChangeHook {

    void onChange(FragmentMeta sourceFragment, List<FragmentMeta> replicaFragments) throws MetaStorageException;

    void onRemove(FragmentMeta sourceFragment);
}
