package com.atguigu.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author name 婉然从物
 * @create 2024-03-16 19:14
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")    // 指定Kafka节点的地址和端口号
                .setGroupId("atguigu")  // 指定消费者组的id
                .setTopics("first")   // 指定消费的主题
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 指定反序列化器， 这里是只反序列化value
                .setStartingOffsets(OffsetsInitializer.latest())    // 指定flink消费Kafka数据的策略
                .build();

        env
//                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource")
                .fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)), "kafkasource")
                .print();

        env.execute();
    }
}

/**
 * kafka消费者的参数:
 *  auto.reset.offsets
 *      earliest: 如果有offset，从offset继续消费:如果没有offset，从  最早  消费
 *      latest  : 如果有offset，从offset继续消费:如果没有offset，从  最新  消费
 * flink的kafkasource,offset消费策略:  offsetsInitializer，默认是 earliest
 *      earliest:  一定从 最早 消费
 *      Latest:    一定从 最新 消费
 */