package com.atguigu.functions;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import sun.management.Sensor;

/**
 * @author name 婉然从物
 * @create 2024-03-17 20:36
 */
public class MapFunctionImpl implements MapFunction<WaterSensor, String> {

    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
