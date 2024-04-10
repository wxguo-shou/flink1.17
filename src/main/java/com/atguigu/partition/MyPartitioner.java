package com.atguigu.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author name 婉然从物
 * @create 2024-03-23 20:38
 */
public class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key) % numPartitions;
    }
}
