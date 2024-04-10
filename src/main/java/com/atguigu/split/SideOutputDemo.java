package com.atguigu.split;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author name 婉然从物
 * @create 2024-03-19 15:46
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());

        /**
         * TODO 需求: 使用侧输出流 实现分流watersensor的数据，s1、s2的数据分别分开
         *
         * 总结步骤
         * 1、使用process算子
         * 2、定义 OutputTag对象
         * 3、调用 ctx.output
         * 4、通过主流 获取 侧流
         *
         * 创建outputTag对象
         * 第一个参数:标签名
         * 第二个参数:放入侧输出流中的 数据的 类型，Typeinformation
         */
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> process = sensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                if ("s1".equals(value.getId())) {
                    /**
                     * 上下文调用ouput，将数据放入侧输出流
                     * 第一个参数:Tag对象
                     * 第二个参数:放入侧输出流中的 数据
                     */
                    ctx.output(s1Tag, value);
                } else if ("s2".equals(value.getId())) {
                    ctx.output(s2Tag, value);
                } else {
                    out.collect(value);
                }
            }
        });

        // 根据标签获取侧输出流
        SideOutputDataStream<WaterSensor> s1 = process.getSideOutput(s1Tag);
        SideOutputDataStream<WaterSensor> s2 = process.getSideOutput(s2Tag);

        // 打印侧输出流
        s1.printToErr("s1");
        s2.printToErr("s2");

        // 打印主流
        process.print("主流");

        env.execute();
    }
}
