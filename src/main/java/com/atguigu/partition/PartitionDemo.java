package com.atguigu.partition;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author name 婉然从物
 * @create 2024-03-19 10:58
 */
public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        // shuffle随机分区
        // return random.nextInt(numberOfChannels);
//        socketDS.shuffle().print();

        // rebalance轮询
        // nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
//        socketDS.rebalance().print();

        // rescale缩放：  实现轮询，  局部组队， 比rebalance高效
//        socketDS.rescale().print();

        // broadcast广播:  发送给下游所有的子任务
        socketDS.broadcast().print();

        // global全局:  只发往第一个子任务
//        return 0;
        socketDS.global().print();


//        keyby:按指定key去发送，相同key发往同一个子任务
//        one-to-one:Forward分区器

//        总结:Flink提供了 7种分区器+1种自定义



        env.execute();
    }
}
