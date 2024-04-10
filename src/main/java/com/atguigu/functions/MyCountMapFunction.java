package com.atguigu.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * @author name 婉然从物
 * @create 2024-03-29 15:42
 */
public class MyCountMapFunction implements CheckpointedFunction, MapFunction<String, Long> {
    private Long count = 0L;
    private ListState<Long> state;

    @Override
    public Long map(String value) throws Exception {
        return ++count;
    }

    /**
     * TODO 2. 本地变量持久化： 将 本地变量  拷贝到  算子状态中
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("snapshotState...");
        // 2.1 清空状态算子
        state.clear();
        // 2.2 将  本地变量 添加到 算子状态中
        state.add(count);
    }

    /**
     * TODO 3. 初始化本地变量：  从状态中， 把数据添加到 本地变量， 每个子任务调用一次
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("initializeState...");
        // 3.1 获取算子状态
        context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<Integer>("state", Types.INT));
//                .getUnionListState(new ListStateDescriptor<Integer>("state-union", Types.INT));

        // 3.2 从  算子状态中 把数据 添加到 本地变量
        if (context.isRestored()) {
            for (Long c : state.get()) {
                count += c;
            }
        }
    }
}
