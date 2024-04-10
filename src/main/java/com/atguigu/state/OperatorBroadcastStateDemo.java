package com.atguigu.state;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.MyCountMapFunction;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author name 婉然从物
 * @create 2024-03-29 15:40
 */
public class OperatorBroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        // 数据流
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());

        // 配置流
        DataStreamSource<String> ConfigDS = env.socketTextStream("hadoop102", 8888);

        // TODO 1. 将配置流广播
        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        BroadcastStream<String> configBS = ConfigDS.broadcast(broadcastMapState);

        // TODO 2. 把数据流 和 广播后的配置流  connect
        BroadcastConnectedStream<WaterSensor, String> sensorBCS = sensorDS.connect(configBS);

        // TODO 3. 调用process
        sensorBCS
                .process(
                new BroadcastProcessFunction<WaterSensor, String, String>() {
                    /**
                     * 数据流的处理方法
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement(WaterSensor value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        // TODO 5. 通过上下文获取广播状态， 取出里面的值（只读， 不能修改）
                        ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                        Integer threshold = broadcastState.get("threshold");
                        // 判断广播状态是否有数据， 因为刚启动时， 可能时数据流的第一条数据先来
                        threshold = threshold == null ? 0 : threshold;
                        if (value.getVc() > threshold) {
                            out.collect(value + ", 水位值超过" + threshold + " !!!");
                        }

                    }

                    /**
                     * 广播后配置流的处理方法
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                        // TODO 4. 通过上下文获取广播状态， 往里面写数据
                        BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                        broadcastState.put("threshold", Integer.valueOf(value));

                    }
                }
        )
                .print();

        env.execute();
    }
}
