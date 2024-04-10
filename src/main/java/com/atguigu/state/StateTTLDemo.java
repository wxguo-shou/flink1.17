package com.atguigu.state;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/** TODO
 * @author name 婉然从物
 * @create 2024-03-26 10:20
 */
public class StateTTLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((WaterSensor element, long recordTimestamp) -> element.getTs() * 1000L));



        sensorDS.keyBy(r -> r.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            ValueState<Integer> lastVcState;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);

                                // TODO 1. 创建stateTtlConfig
                                StateTtlConfig stateTTLConfig = StateTtlConfig
                                        .newBuilder(Time.seconds(5))    // 过期时间5s
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)    // 状态 创建 和写入（更新） 更新过期时间
                                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)    // 状态 读取、创建 和写入（更新） 更新过期时间
//                                        .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) // 返回没被清理的过期状态值
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 不返回过期的状态值
                                        .build();

                                // TODO 2. 状态描述器， 启用 TTL
                                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("lastVcState", Types.INT);
                                stateDescriptor.enableTimeToLive(stateTTLConfig);

                                this.lastVcState = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                // 先获取状态值， 打印
                                Integer lastVc = lastVcState.value();
                                out.collect("key=" + value.getId() + ",状态值=" + lastVc);

                                // 更新状态值
                                if (value.getVc()>10) {
                                    lastVcState.update(value.getVc());
                                }
                            }
                        }
                )
                .print();

        env.execute();
    }
}
