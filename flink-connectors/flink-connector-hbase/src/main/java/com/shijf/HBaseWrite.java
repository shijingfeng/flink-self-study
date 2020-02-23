package com.shijf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.shijf.HBaseConstant.*;

/**
 * @description:
 * @author: sjf
 * @time: 2020/2/19 14:42
 */
public class HBaseWrite extends RichSinkFunction<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseWrite.class);

    private static byte[] CF_BYTES = Bytes.toBytes("cf_info");
    private static TableName TABLE_NAME = TableName.valueOf("shijingfeng_db:t1");
    private Connection connection = null;
    private org.apache.hadoop.conf.Configuration configuration;
    private Table table = null;


    // RichSinkFunction.open()方法是按并行度执行的，而创建HBase连接是个很贵的操作
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //创建hbase连接配置
        configuration = HBaseConfiguration.create();

        configuration.set(HBASE_ZOOKEEPER_QUORUM, "192.168.89.128,192.168.89.129,192.168.89.130,192.168.89.131");
        configuration.set(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, "2181");
        configuration.set(HBASE_RPC_TIMEOUT, "30000");
        configuration.set(HBASE_CLIENT_OPERATION_TIMEOUT, "30000");
        configuration.set(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "30000");

        //创建连接
        connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TABLE_NAME);


        LOGGER.info("Opened HBaseSink");
    }

    // 数据处理-添加数据到hbase
    @Override
    public void invoke(String record, Context context) throws Exception {
        // 为了提高写入效率，在并发大时还可以使用HBase的BufferedMutator
        String[] split = record.split(",");
        //构建hbase插入对象
        Put put = new Put(Bytes.toBytes(split[0]));
        put.addColumn(CF_BYTES, Bytes.toBytes("name"), Bytes.toBytes(split[1]));

        table.put(put);
    }

    @Override
    public void close() {
        try {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            LOGGER.error("Close HBase Exception:", e.toString());
        }
    }
}
