package com.shijf.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * flink kafka连接器
 * demo 从kafka数据中读取数据，词频词频统计,写入本地文件
 *
 *@description:
 *@author: sjf
 *@time: ${DATE} ${TIME}
 *
 */
public class KafkaConsumer {
    public static void main(String[] args) {
        //创建流处理上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000); //启用检查点，每五秒检查

        //从kafka中获取数据流
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.89.128:9092");
        props.setProperty("group.id", "flink-group1");

        //key-value序列化类型
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        /*earliest
        当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
          latest
        当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        */
        props.setProperty("auto.offset.reset", "latest");

        //监听kafka192.168.89.128:9092  topic为collect
        //kafka的topic支持传多个topic,也就是支持同时消费多个topic

        //SimpleStringSchema 是一种反序列化类型，为了访问Kafka消息的键、值和元数据使用不用的反序列化
        //SimpleStringSchema 将数据反序列化为String类型 用户获取kafka的value

        // TypeInformationSerializationSchema 对Flink原生支持数据类型的反序列化，获取kafka的key和value
        //new TypeInformationSerializationSchema<Tuple2<String,String>>(TypeInformation.of(new TypeHint<Tuple2<String,String>>() {}), env.getConfig());

        //JsonDeserializationSchema实现对JSON格式数据的反序列化 读取到的数据在 value 字段中，对应的元数据在 metadata 字段中
        // new JSONKeyValueDeserializationSchema(true) //可以控制是否需要元数据字段
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<>("collect", new SimpleStringSchema(), props));


        //Kafka使用者允许配置如何确定Kafka分区的起始位置。
        //        myConsumer.setStartFromEarliest();     //尽可能从最早的记录开始
        //        myConsumer.setStartFromLatest();       // 尽可能从最新的记录开始
        //        myConsumer.setStartFromTimestamp(...); // 从指定的epoch时间戳开始(毫秒)
        //        myConsumer.setStartFromGroupOffsets(); // 如果不设置，默认从offsets开始
        kafkaStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                 for(String entity:s.split(",")){
                     collector.collect(new Tuple2<String,Integer>(entity,1));
                 }
            }
        }).keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                return new Tuple2<String,Integer>(t1.f0,t1.f1+stringIntegerTuple2.f1);
            }
        }).writeAsText("D:\\bigdata_software\\bigdata_data\\kafkaTest1").setParallelism(1);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
