package com.atguigu.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;
import static org.apache.flink.table.api.Expressions.$;

/**
 * @author name 婉然从物
 * @create 2024-04-05 11:04
 */
public class MyTableAggregateFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> numDS = env.fromElements(3, 6, 12, 5, 8, 9, 4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table numTable = tableEnv.fromDataStream(numDS, $("num"));

        // TODO 2. 注册函数
        tableEnv.createTemporaryFunction("Top2", Top2.class);

        // TODO 3. 调用自定义函数  只支持 Table API

        numTable
//                .groupBy()
                .flatAggregate(
                        Expressions.call("Top2", $("num")).as("value", "rank")
                )
                .select($("value"), $("rank"))
                .execute()
                .print();
    }

    // TODO 1.继承 TableAggregateFunction<返回类型，累加器类型<加权总和，权重总和>>
    // 返回类型  (数值，排名) =》 (12,1)(9,2)
    // 累加器类型  (第一大的数，第二大的数)
    public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>{

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }

        /**
         *
         * @param acc   累加器
         * @param num   来的数据
         */
        public void accumulate(Tuple2<Integer, Integer> acc, Integer num){
            if (num > acc.f0){
                acc.f1 = acc.f0;
                acc.f0 = num;
            } else if (num > acc.f1){
                acc.f1 = num;
            }
        }

        /**
         *
         * @param acc   累加器
         * @param out   采集器
         */
        public void emitValue(Tuple2<Integer, Integer> acc, Collector<Tuple2<Integer, Integer>> out){
            if (acc.f0 != 0){
                out.collect(Tuple2.of(acc.f0, 1));
            }

            if (acc.f1 != 0){
                out.collect(Tuple2.of(acc.f1, 2));
            }
        }
    }
}