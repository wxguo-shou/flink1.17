package com.atguigu.state;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** TODO 检测每种传感器的水位值， 如果连续的两个水位值超过10， 就输出报警
 * @author name 婉然从物
 * @create 2024-03-26 10:20
 */
public class KeyedListStateDemo {
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

                            ListState<Integer> vcListState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                vcListState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("vcListState", Types.INT));
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                // 1. 来一条存到list状态里
                                vcListState.add(value.getVc());

                                // 2. 从list状态拿出来， 拷贝到一个list中， 排序， 只留3个最大的
                                Iterable<Integer> vcListIt = vcListState.get();
                                List<Integer> vcList = new ArrayList<>();
                                for (Integer vc : vcListIt) {
                                    // 2.1 拷贝到list中
                                    vcList.add(vc);
                                }
                                // 2.2 对list进行降序排序
                                vcList.sort((o1, o2) -> o2 - o1);
                                // 2.3 只保留最大的3个
                                if (vcList.size() > 3) {
                                    vcList.remove(3);
                                }

                                out.collect("传感器id为" + value.getId() + ", 最大的3个水位值=" + vcList.toString());

                                // 3. 更新list状态
                                vcListState.update(vcList);

//                                vcListState.get();      // 取出 list状态 本组的数据， 是一个Iterable
//                                vcListState.add();      // 向list状态 本组 添加一个元素
//                                vcListState.addAll();   // 向list状态 本组 添加多个元素
//                                vcListState.update();   // 更新list状态， 本组数据（覆盖）
//                                vcListState.clear();    // 清空list状态  本组数据
                            }
                        }
                )
                .print();

        env.execute();
    }
}
