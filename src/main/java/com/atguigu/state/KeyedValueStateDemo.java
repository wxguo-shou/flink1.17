package com.atguigu.state;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/** TODO 检测每种传感器的水位值， 如果连续的两个水位值超过10， 就输出报警
 * @author name 婉然从物
 * @create 2024-03-26 10:20
 */
public class KeyedValueStateDemo {
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
                            // TODO 1. 定义状态
                            ValueState<Integer> lastVcState;    // 不能定义普通数据， 普通数据值的变化不会区分不同key
//                            ValueState<Integer> lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Types.INT));
                            // Exception in thread "main" java.lang.IllegalStateException: The runtime context has not been initialized.

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                // TODO 2. 在open方法中， 初始化状态
                                // 状态描述器两个参数:  第一个参数，起个名字，唯一不重复;  第二个参数，存的类型
                                lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Types.INT));
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
//                                lastVcState.value();  // 取出值状态里的数据
//                                lastVcState.update();  // 更新值状态里的数据
//                                lastVcState.clear();   // 清空值状态里的数据

                                // 1. 取出上一条数据的水位值(Integer默认值是null， 所以要判断)
                                int lastVc = lastVcState.value() == null ? 0 : lastVcState.value();

                                // 2. 求差值的绝对值， 判断是否超过10
                                Integer vc = value.getVc();
                                if (Math.abs(vc - lastVc) > 10) {
                                    out.collect("传感器ID=" + value.getId() + "当前水位值=" + vc + ", 与上一条水位值" + lastVc + ", 之差大于10！！！");
                                }

                                // 3. 更新状态里的水位值
                                lastVcState.update(vc);
                            }
                        }
                )
                .print();

        env.execute();
    }
}
