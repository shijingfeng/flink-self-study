package com.shijf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:
 * @author: sjf
 * @time: 2020/2/18 23:09
 */
public class HBaseSourceTask {

    public static void main(String[] args) {

        //创建流处理上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置检查点
        env.enableCheckpointing(5000);

        env.addSource(new HBaseReader<>()).map(new MapFunction<Tuple2<String, String>, String>() {
            @Override
            public String map(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2.f1;
            }
        }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
