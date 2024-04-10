package com.atguigu.checkpoint;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/** TODO
 * @author name 婉然从物
 * @create 2024-03-26 10:20
 */
public class SavePointDemo {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        System.setProperty("HADOOP_USER_NAME", "wanxiangguo");

        // TODO 检查点配置
        // chk恢复的时候不能切换状态后端， 而且路径必须要指定到 chk-xxx

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/chk");

        checkpointConfig.setCheckpointTimeout(60000);

        checkpointConfig.setMaxConcurrentCheckpoints(1);

        checkpointConfig.setMinPauseBetweenCheckpoints(1000);

        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        checkpointConfig.setTolerableCheckpointFailureNumber(10);

        // TODO 开启 非对齐检查点 (barrier非对齐)

        checkpointConfig.enableUnalignedCheckpoints();
        // 开启非对齐检查点才生效： 默认0， 表示一开始就直接用  非对齐的检查点
        // 如果大于0， 一开始用 对齐的检查点（barrier对齐）， 对齐的时间超过这个参数， 自动切换成 非对齐检查点（barrier非对齐）
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1));

        env.socketTextStream("hadoop102", 7777).uid("socket")
        .flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                    // TODO 3.1 切分数据
                    String[] words = s.split(" ");
                    // TODO 3.2 转化为二元组
                    for (String word : words) {
                        Tuple2<String, Integer> wordAndOne = Tuple2.of(word, 1);
                        collector.collect(wordAndOne);
                    }
                }
        )
                .uid("flatmap-wc").name("wc-flatmap")
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(stringIntegerTuple2 -> stringIntegerTuple2.f0)
                .sum(1).uid("sum-wc")
                .print().uid("print-wc");

        env.execute();
    }
}
