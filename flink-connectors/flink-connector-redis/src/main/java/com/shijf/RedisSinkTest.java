package com.shijf;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @description:
 * @author: sjf
 * @time: 2020/2/19 15:54
 */
public class RedisSinkTest<T extends Tuple> {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Tuple2<String, String>> product = env.readTextFile("d://text.txt")
                .map(string -> {
                    String[] split = string.split(",");
                    Tuple2<String, String> stringStringTuple2 = new Tuple2<>(split[0], split[1]);
                    return stringStringTuple2;
                });
//        product.print();

        //单个 Redis
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.89.128").setPort(6379).build();
        RedisSink<Tuple2<String, String>> stringRedisSink = new RedisSink<Tuple2<String, String>>(conf, new RedisSinkMapper());
        product.addSink(stringRedisSink);

        //Redis 的 ip 信息一般都从配置文件取出来
        //Redis 集群
/*        FlinkJedisClusterConfig clusterConfig = new FlinkJedisClusterConfig.Builder()
                .setNodes(new HashSet<InetSocketAddress>(
                        Arrays.asList(new InetSocketAddress("redis1", 6379)))).build();*/

        //Redis Sentinels
/*        FlinkJedisSentinelConfig sentinelConfig = new FlinkJedisSentinelConfig.Builder()
                .setMasterName("master")
                .setSentinels(new HashSet<>(Arrays.asList("sentinel1", "sentinel2")))
                .setPassword("")
                .setDatabase(1).build();*/

        try {
            env.execute("flink redis connector");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class RedisSinkMapper implements RedisMapper<Tuple2<String, String>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "shijingfeng");
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}
