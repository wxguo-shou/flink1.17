package com.atguigu.sink;

import com.atguigu.source.DataGeneratorDemo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @author name 婉然从物
 * @create 2024-03-20 10:37
 */
public class SinkFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 每个目录中，  都有并行度个文件在写入
        env.setParallelism(2);

        // 必须开启checkpoint, 否则一直都是.inprogress
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                r -> "Number:" + r,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100),
                Types.STRING
        );

        DataStreamSource<String> dataGen = env
                .fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");

        // TODO 输出到文件系统
        FileSink<String> fileSink = FileSink
                // 输出行式存储的文件，  指定路径、  指定编码
                .<String>forRowFormat(new Path("e:/tmp"), new SimpleStringEncoder<>("UTF-8"))
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("atguigu")
                        .withPartSuffix(".log")
                        .build()
                )
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // 设置文件滚动策略  10s,  1k
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(new MemorySize(1024))
                                .build())
                .build();

        dataGen.sinkTo(fileSink);


        env.execute();
    }
}
