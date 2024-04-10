package com.atguigu.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @author name 婉然从物
 * @create 2024-03-20 16:58
 */
public class SinkKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 必须开启checkpoint, 否则一直都是.inprogress
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> sensorDS = env.socketTextStream("hadoop102", 7777);

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // 指定Kafka的地址和端口
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                // 指定序列化器：  指定topic名称、 具体的序列化
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("ws")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                // 写道Kafka的一致性级别： 精准一次， 至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 如果是精准一次， 必须设置  事务的前缀
                .setTransactionalIdPrefix("atguigu-")
                // 如果是精准一次， 必须设置  事务的超时时间： 要大于checkpoint间隔时间, 小于 max  15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10*60*1000+"")
                .build();

        sensorDS.sinkTo(kafkaSink);

        env.execute();
    }
}
