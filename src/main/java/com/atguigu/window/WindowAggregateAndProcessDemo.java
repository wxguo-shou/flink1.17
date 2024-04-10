package com.atguigu.window;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author name 婉然从物
 * @create 2024-03-22 10:01
 */
public class WindowAggregateAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(r -> r.getId());

        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // 2.窗口函数: 增量聚合 Aggregate
        sensorWS.aggregate(
                new MyAgg(),
                new MyProcess()

        )
                .print();


        env.execute();
    }

    public static class MyAgg implements AggregateFunction<WaterSensor, Integer, String>{

        @Override
        public Integer createAccumulator() {
            System.out.println("创建累加器");
            return 0;
        }

        /**
         * 聚合逻辑
         * @param value
         * @param accumulator
         * @return
         */
        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            System.out.println("调用add方法, value=" + value);
            return accumulator + value.getVc();
        }

        /**
         * 获取最终结果， 窗口触发时输出
         * @param accumulator
         * @return
         */
        @Override
        public String getResult(Integer accumulator) {
            System.out.println("调用getResult方法");
            return accumulator.toString();
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            // 只有会话窗口才会用到
            System.out.println("调用merge方法");
            return null;
        }
    }

    public static class MyProcess extends ProcessWindowFunction<String, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            long startTs = context.window().getStart();
            long endTs = context.window().getEnd();
            String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
            String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

            long count = elements.spliterator().estimateSize();
            out.collect("key="+s+"的窗口["+windowStart+","+windowEnd+")包含"+count+"条数据===>"+elements.toString());
        }
    }


}

/**
 * 触发器、移除器:  现成的几个窗口，都有默认的实现，一般不需要自定义
 *
 * 以 时间类型的 滚动窗口 为例，分析原理:
 * TODO 1、窗口什么时候触发 输出?
 *          时间进展>= 窗口的最大时间戳(end -1ms)
 *
 * TODO 2、窗口是怎么划分的?
 *      start=向下取整，取窗口长度的整数倍
 *      end = start + 窗口长度
 *
 *      窗口左闭右开 ==》属于本窗口的 最大时间戳=end - 1ms
 *
 * TODO 3、窗口的生命周期?
 *      创建: 属于本窗口的第一条数据来的时候，现new的，放入一个singleton单例的集合中
 *      销毁(关窗):时间进展 >= 窗口的最大时间戳(end - 1ms) + 允许迟到的时间(默认0)
 */
