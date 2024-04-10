package com.atguigu.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author name 婉然从物
 * @create 2024-04-05 11:04
 */
public class MyAggregateFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple3<String, Integer, Integer>> sensorWeightDS = env.fromElements(
                Tuple3.of("zs", 98, 3),
                Tuple3.of("zs", 90, 3),
                Tuple3.of("zs", 85, 4),
                Tuple3.of("ls", 92, 3),
                Tuple3.of("ls", 87, 3),
                Tuple3.of("ls", 95, 4)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table sensorWeightTable = tableEnv
                .fromDataStream(sensorWeightDS, $("f0").as("name"),
                        $("f1").as("score"), $("f2").as("weight"));
        tableEnv.createTemporaryView("scores", sensorWeightTable);

        // TODO 2. 注册函数
        tableEnv.createTemporaryFunction("WeightedAvg", WeightedAvg.class);

        // TODO 3. 调用自定义函数
        tableEnv
                .sqlQuery("select name, WeightedAvg(score, weight) as WeightedAvg from scores group by name")
                .execute()
                .print();
    }

    // TODO 1. 继承 AggregateFunction <返回类型， 累加器类型<加权总和， 权重总和> >
    public static class WeightedAvg extends AggregateFunction<Double, Tuple2<Integer, Integer>>{

        @Override
        public Double getValue(Tuple2<Integer, Integer> accumulator) {
            return accumulator.f0 * 1D / accumulator.f1;
        }

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }

        /**
         * 累加计算的方法， 每个方法来都会调用一次
         * @param acc       累加器
         * @param score     分数
         * @param weight    权重
         */
        public void accumulate(Tuple2<Integer, Integer> acc, Integer score, Integer weight){
            acc.f0 += score * weight;
            acc.f1 += weight;
        }
    }
}
