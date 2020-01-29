package com.shijf.study.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkSocket {

    public static void main(String[] args) throws Exception {
        //参数检查
        if (args.length != 2) {
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }

        //获取socket ip和端口
        String hostname = args[0];
        Integer port = Integer.parseInt(args[1]);

        //获取流数据上下文
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据
        DataStreamSource<String> stream = env.socketTextStream(hostname, port);
        //计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>(){

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = s.toLowerCase().split("\\W+");

                for (String token: tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }
            }
        })
                .keyBy(0)
                .sum(1);
        sum.print();
        env.execute("Java WordCount from SocketText");
    }
}
