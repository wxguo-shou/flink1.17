package com.atguigu.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author name 婉然从物
 * @create 2024-03-19 10:58
 */
public class PartitionCustomDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        socketDS.partitionCustom(

            (String key, int numPartitions)  -> Integer.parseInt(key) % numPartitions
                , r -> r).print();

        env.execute();
    }
}