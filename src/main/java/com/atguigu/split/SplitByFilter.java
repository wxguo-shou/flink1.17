package com.atguigu.split;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author name 婉然从物
 * @create 2024-03-19 15:40
 */
public class SplitByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        // TODO 使用Filter来实现
        // 缺点： 同一个数据， 要被处理两遍
        socketDS.filter(value -> Integer.valueOf(value) % 2 == 0).print("偶数流");
        socketDS.filter(value -> Integer.valueOf(value) % 2 == 1).print("奇数流");

        env.execute();
    }
}
