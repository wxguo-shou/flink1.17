package com.atguigu.aggregate;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * @author name 婉然从物
 * @create 2024-03-17 20:24
 */
public class SimpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        /**
         * TODO 简单聚合算子：
         *  1、 keyBy之后才会调用
         *  2、 分组内的聚合： 对同一个key的数据进行聚合
         */

        KeyedStream<WaterSensor, Object> sensorKS = sensorDS.keyBy(value -> value.getId());

        // 传位置索引的， 只适合Tuple， 不适合POJO
        /**
         *  max\mgxby的区别:  同min
         *      max: 只会取比较字段的最大值，非比较字段保留第一次的值
         *      maxBy: 取比较字段的最大值，同时非比较字段 取最大值这条数据的值

         */
        SingleOutputStreamOperator<WaterSensor> result = sensorKS.maxBy("vc");
//        SingleOutputStreamOperator<WaterSensor> result = sensorKS.max("vc");

        result.print();

        env.execute();
    }
}

