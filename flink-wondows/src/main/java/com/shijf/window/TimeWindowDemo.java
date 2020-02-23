package com.shijf.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @description:
 * @author: sjf
 * @time: 2020/2/23 13:10
 */
public class TimeWindowDemo {

    public static void main(String[] args) {

        //创建执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        //如果不指定时间的话，默认是 ProcessingTime，但是如果指定为事件事件的话，需要事件中带有时间或者添加时间水印
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //设置并行度
        env.setParallelism(1);

        //设置socket数据源
        DataStreamSource<String> streamData = env.socketTextStream("cluster01", 9999);

        //如果operate中使用lambda需要指定返回类型，不然会报错。尽量还是使用匿名类
        //tuple2有两个包中有scala和org.apache.flink.api.java.tuple   引用错的话会报错
        streamData.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, org.apache.flink.util.Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split("\\W+");
                for(String entity : split){
                    collector.collect(new Tuple2<String,Integer>(entity,1));
                }
            }
        }).keyBy(0)
                //timewindow(Time size, Time slide)  如果传slides 是滑动窗口 如果不传是反转窗口
                .timeWindow(Time.seconds(2))
                //.timeWindow(Time.seconds(5),Time.seconds(2))
                .trigger(new CustomTrigger())
                .sum(1)
                .print()
        ;
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
