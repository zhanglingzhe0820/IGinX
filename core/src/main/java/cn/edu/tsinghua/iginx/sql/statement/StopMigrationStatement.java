package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.utils.RpcUtils;

public class StopMigrationStatement extends SystemStatement {
    public StopMigrationStatement() {
        this.statementType = StatementType.STOP_MIGRATION;
    }

    @Override
    public void execute(RequestContext ctx) throws ExecutionException {
        ConfigDescriptor.getInstance().getConfig().setEnableDynamicMigration(!ConfigDescriptor.getInstance().getConfig().isEnableDynamicMigration());
        ctx.setResult(new Result(RpcUtils.SUCCESS));
    }
}
