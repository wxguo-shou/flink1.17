package com.atguigu.watermark;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author name 婉然从物
 * @create 2024-03-22 10:01
 */
public class WaterMarkOutOfOrdernessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);

        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());



        // TODO 指定Watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // 1.1 指定watermark生成：  乱序，  等待3s
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 1.2 指定  时间戳分配器，  从数据中提取
                .withTimestampAssigner(
                          (element, recordTimestamp) -> {
                            // 返回的时间戳   要毫秒
                            System.out.println("数据=" + element + ", recordTs=" + recordTimestamp);
                            return element.getTs() * 1000L;
                        }
                );

        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);

        sensorDSwithWatermark.keyBy(r -> r.getId())
                // 使用事件语义窗口
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        // 上下文可以拿到window对象， 还有其他东西： 侧输出流  等等
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                        long count = elements.spliterator().estimateSize();
                        out.collect("key="+s+"的窗口["+startTs+","+endTs+")包含"+count+"条数据===>"+elements.toString());
                    }
                }
        )
                .print();

        env.execute();
    }
}
/**
 TODO 内置Watermark的生成原理
 1、都是周期性生成的：  默认200ms
 2、有序流： Watermark = 当前最大的事件时间  -  1ms
 3、乱序流： Watermark = 当前最大的事件时间  -  延迟时间  -  1ms
 env.getConfig().setAutoWatermarkInterval()
 */