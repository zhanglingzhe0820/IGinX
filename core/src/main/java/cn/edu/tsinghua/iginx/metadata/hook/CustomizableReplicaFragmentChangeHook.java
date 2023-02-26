package cn.edu.tsinghua.iginx.metadata.hook;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;

import java.util.List;

public interface CustomizableReplicaFragmentChangeHook {

    void onChange(FragmentMeta sourceFragment, List<FragmentMeta> replicaFragments);

    void onRemove(FragmentMeta sourceFragment);
}
