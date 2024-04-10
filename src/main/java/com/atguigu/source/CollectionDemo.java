package com.atguigu.source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author name 婉然从物
 * @create 2024-03-16 19:06
 */
public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4));

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4);

        source.print();

        env.execute();
    }
}
