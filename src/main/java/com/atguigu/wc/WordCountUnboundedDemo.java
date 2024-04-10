package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author name 婉然从物
 * @create 2024-03-15 18:34
 */
public class WordCountUnboundedDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 2. 读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        // TODO 3. 处理数据, 输出
        socketDS.flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                // TODO 3.1 切分数据
                String[] words = s.split(" ");
                // TODO 3.2 转化为二元组
                for (String word : words) {
                    Tuple2<String, Integer> wordAndOne = Tuple2.of(word, 1);
                    collector.collect(wordAndOne);
                }
            }
        ).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(stringIntegerTuple2 -> stringIntegerTuple2.f0)
                .sum(1)
                .print();

        // TODO 5. 执行
        env.execute();
    }
}
