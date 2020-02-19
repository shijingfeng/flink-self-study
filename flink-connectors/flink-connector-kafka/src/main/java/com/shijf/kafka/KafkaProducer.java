package com.shijf.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 *
 * flink kafka连接器
 * demo 从文本文件中读取数据，写入kafka
 *
 * @description:
 * @author: sjf
 * @time: 2020/2/17 22:49
 */
public class KafkaProducer {

    public static void main(String[] args) {
        //创建流处理上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取本地文件
        DataStreamSource<String> stream = env.readTextFile("D:\\bigdata_software\\bigdata_data\\kafkaTest");

        //这个在代码已经不推荐使用，但是官网文档还在用这个。说明还是ok的
        //添加kafka_sink
        stream.addSink(new FlinkKafkaProducer<String>(
                "192.168.89.128:9092",
                "write",
                new SimpleStringSchema()
        ));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
