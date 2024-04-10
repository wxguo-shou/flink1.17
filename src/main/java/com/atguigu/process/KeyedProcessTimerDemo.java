package com.atguigu.process;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import sun.management.Sensor;

import java.time.Duration;

/**
 * @author name 婉然从物
 * @create 2024-03-26 10:20
 */
public class KeyedProcessTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());

        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, recordTimestamp) ->
                        element.getTs() * 1000L
                );

        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);

        KeyedStream<WaterSensor, String> sensorKS = sensorDSwithWatermark.keyBy(r -> r.getId());

        // TODO Process: keyed
        SingleOutputStreamOperator<String> process = sensorKS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {


                    /**
                     * 来一条数据调用一次
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 获取当前数据的key
                        String currentKey = ctx.getCurrentKey();

                        // TODO 1. 定时器注册
                        TimerService timerService = ctx.timerService();
                        // 1、 事件时间案例
                        Long currentEventTime = ctx.timestamp();
//                        timerService.registerEventTimeTimer(5000L);
//                        System.out.println("当前key=" + currentKey +",当前时间是=" + currentEventTime + ",注册了一个5s的定时器");

                        // 2、 处理时间案例
//                        long currentTs = timerService.currentProcessingTime();
//                        timerService.registerProcessingTimeTimer(currentTs + 5000L);
//                        System.out.println("当前key=" + currentKey +",当前时间是=" + currentTs + ",注册了一个5s的定时器");

                        // 3、 获取 process 的 当前 watermark
                        long currentWatermark = timerService.currentWatermark();
                        System.out.println("当前数据=" + value + ",当前watermark=" + currentWatermark);

                        // 注册定时器： 处理时间
//                        timerService.registerProcessingTimeTimer();
                        // 删除定时器: 事件时间
//                        timerService.deleteEventTimeTimer();
                        // 删除定时器： 处理时间
//                        timerService.deleteProcessingTimeTimer();
                        // 获取当前的 watermark
//                        long wm = timerService.currentWatermark();
                    }

                    /**
                     * TODO 2. 时间进展到定时器触发的时间， 调用该方法
                     * @param timestamp 当前进展时间
                     * @param ctx       上下文
                     * @param out       采集器
                     * @throws Exception
                     */
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        String currentKey = ctx.getCurrentKey();
                        System.out.println("当前key=" + currentKey + ",现在时间=" + timestamp + ",定时器触发");
                    }
                }
        );

        process.print();

        env.execute();
    }
}
/**
 TODO 定时器
 1、keved才有
 2、事件时间定时器，通过watermark来触发的
    watermark >= 注册的时间
    注意: watermark = 当前最大事件时间 - 等待时间 - 1ms, 因为 -1ms，所以会推迟一条数据
        比如，5s的定时器，
        如果等待=3s，watermark = 8s - 3s - 1ms = 4999ms,不会触发5s的定时器
        需要 watermark = 9s - 3s -1ms = 5999ms ，才能去触发5s的定时器
 3、在process中获取当前watermark，显示的是上一次的wgtermark
    =》因为process还没接收到这条数据对应生成的新watermark
 */