package com.shijf.study.api.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;

public class SourceAPI {
    public static void main(String[] args) throws Exception {
        //创建流处理上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //丛集合中获取source
        ArrayList<Integer> data = new ArrayList<>();
        data.add(10);
        data.add(15);
        data.add(20);
        DataStreamSource<Integer> integerDataStreamSource = env.fromCollection(data);
        DataStreamSource<Integer> integerDataStreamSource1 = env.fromElements(1, 2, 3);


        //从文件创建数据流
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("/opt/text.txt");

        //从sockect中读取数据
        DataStreamSource<String> localhost = env.socketTextStream("localhost", 9999, ",");

        //从kafka中获取数据流
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.47.85.158:9092");
        props.setProperty("group.id", "flink-group");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<>("mastertest", new SimpleStringSchema(), props));

        //自定义mysqlsource
        env.addSource(new SourceFromMySQL()).print();
        env.execute("Flink Source ");
    }


    /**
    * Description: MysqlSource实现
    * @author: shijf
    * @date:
    * @param:
    * @return:
    */
    public static class SourceFromMySQL extends RichSourceFunction<String> {
        PreparedStatement ps;
        private Connection connection;

        /**
         * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = getConnection();
            String sql = "select * from Student;";
            ps = this.connection.prepareStatement(sql);
        }

        /**
         * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
         *
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            super.close();
            if (connection != null) { //关闭连接和释放资源
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        }

        /**
         * DataStream 调用一次 run() 方法用来获取数据
         *
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String  student = resultSet.getString("name").trim();
                ctx.collect(student);
            }
        }

        @Override
        public void cancel() {
        }

        private  Connection getConnection() {
            Connection con = null;
            try {
                Class.forName("com.mysql.jdbc.Driver");
                con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "123456");
            } catch (Exception e) {
                System.out.println("mysql get connection has exception , msg = " + e.getMessage());
            }
            return con;
        }
    }
}
