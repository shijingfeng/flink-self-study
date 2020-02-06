package com.shijf.study.api.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SinkSource {
    public static final String HBASE_TABLE_NAME = "hbase_sink";

    public static void main(String[] args) throws IOException {
        //创建流上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取socket数据形成数据流
        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);


        //kafka sink
        stream.addSink(new FlinkKafkaProducer<String>("localhost:9092",
                "test", new SimpleStringSchema()));

        //redis sink
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        stream.map(new MapFunction<String, Tuple2<String, String>>() {

            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return new Tuple2<>(s,s);
            }
        }).addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper()));


        //ES sink
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200));
        httpHosts.add(new HttpHost("10.2.3.1", 9200));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<String>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element);

                        return Requests.indexRequest()
                                .index("my-index")
                                .type("my-type")
                                .source(json);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        // 批量请求的配置;这指示接收器在每个元素之后发出，否则它们将被缓冲
        esSinkBuilder.setBulkFlushMaxActions(1);

        // finally, build and add the sink to the job's pipeline
        stream.addSink(esSinkBuilder.build());

        //hbase sink
        Job job = Job.getInstance();
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, HBASE_TABLE_NAME);
        stream.output(new HadoopOutputFormat<Text, Mutation>(new TableOutputFormat<Text>(), job));

        //自定义mysql sink
        stream.addSink(new SinkToMySQL());


    }

    /**
    * Description: redis sink
    * @author:
    * @date:
    * @param:
    * @return:
    */
    public static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME");
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


    public static class SinkToMySQL extends RichSinkFunction<String> {
        PreparedStatement ps;
        private Connection connection;

        /**
         * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = getConnection();
            String sql = "insert into Student(id, name, password, age) values(?, ?, ?, ?);";
            ps = this.connection.prepareStatement(sql);
        }

        @Override
        public void close() throws Exception {
            super.close();
            //关闭连接和释放资源
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        }

        /**
         * 每条数据的插入都要调用一次 invoke() 方法
         *
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(String value, Context context) throws Exception {
            //组装数据，执行插入操作
            ps.setString(1, value);

            ps.executeUpdate();
        }

        private static Connection getConnection() {
            Connection con = null;
            try {
                Class.forName("com.mysql.jdbc.Driver");
                con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "root123456");
            } catch (Exception e) {
                System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
            }
            return con;
        }
    }
}
