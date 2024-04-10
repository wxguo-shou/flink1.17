package com.atguigu.state;

import com.atguigu.functions.MyCountMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author name 婉然从物
 * @create 2024-03-29 15:40
 */
public class OperatorListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .setParallelism(2)
                .socketTextStream("hadoop102", 7777)
                .map(new MyCountMapFunction())
                .print();

        env.execute();
    }
}
/**
 * 算子状态中，list 与unionlist的区别:  并行度改变时，怎么重新分配状态
 * 1、list状态:、轮询均分  给  新的  并行子任务
 * 2、unionlist状态: 原先的多个子任务的状态， 合并成一份完整的。会把 完整的列表 广播给 新的并行子任务 (每人一份完整的)
 */