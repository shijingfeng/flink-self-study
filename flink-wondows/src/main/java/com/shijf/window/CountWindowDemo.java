package com.shijf.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:
 * @author: sjf
 * @time: 2020/2/23 14:14
 */
public class CountWindowDemo {
    public static void main(String[] args) {
        //创建执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

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
        })//.countWindowAll(5)  //countwindowAll 统计的是所有数据条数
                .keyBy(0)
                //countwindow(Long size, long slide)  如果传slides 是滑动窗口 如果不传是反转窗口
                //计数窗口计的是统计的key对应数量，不是处理的条数。比如key为1的数据达到5条后触发窗口
                //而不是进来五条数据就触发计数窗口
                .countWindow(5)
                //.countWindow(5,2)
                .sum(1)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
