package com.shijf.study.api.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class Environment {
    public static void main(String[] args) {

        //创建批处理上下文
        //ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        //返回本地环境
        //LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1);

        //返回集群环境
        /*ExecutionEnvironment remoteEnvironment = ExecutionEnvironment.createRemoteEnvironment("jobmanage-hostname",
                6123, "YOURPATH//wordcount.jar");*/

        //创建流处理上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
