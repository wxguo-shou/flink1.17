package com.atguigu.state;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

/** TODO 统计每种传感器每种水位值出现的次数
 * @author name 婉然从物
 * @create 2024-03-26 10:20
 */
public class KeyedReducingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L));


        sensorDS.keyBy(r -> r.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            ReducingState<Integer> vcSumRedusingState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                vcSumRedusingState = getRuntimeContext()
                                        .getReducingState(
                                                new ReducingStateDescriptor<Integer>(
                                                        "vcSumRedusingState",
                                                        (value1, value2) -> value1 + value2,
                                                        Types.INT
                                                )
                                        );
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                vcSumRedusingState.add(value.getVc());
                                Integer vcSum = vcSumRedusingState.get();
                                out.collect("传感器id=" + value.getId() + ", 水位值总和=" + vcSum);

//                                vcSumRedusingState.get();   // 对本组的Redusing状态， 获取结果
//                                vcSumRedusingState.add();   // 对本组的Redusing状态， 添加数据
//                                vcSumRedusingState.clear(); // 对本组的Redusing状态， 清空数据
                            }
                        }
                )
                .print();

        env.execute();
    }
}
