package com.shijf;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.Iterator;

import static com.shijf.HBaseConstant.*;

/**
 * @description:
 * @author: sjf
 * @time: 2020/2/18 21:32
 */
public  class HBaseReader<run> extends RichSourceFunction<Tuple2<String, String>> {
    private static final Logger logger = LoggerFactory.getLogger(HBaseReader.class);

    private org.apache.hadoop.conf.Configuration configuration;
    private Connection conn = null;
    private Table table = null;
    private Scan scan = null;
    private Admin admin = null;



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

        TableName tableName = TableName.valueOf("shijingfeng_db:t1");
        String cf1 = "cf_info";

        Connection conn = ConnectionFactory.createConnection(configuration);
        admin = conn.getAdmin();
        if (!admin.tableExists(tableName)) { //检查是否有该表，如果没有，创建
            logger.info("==============不存在表 = {}", tableName);
            admin.createTable(new HTableDescriptor(TableName.valueOf("shijingfeng_db:t1"))
                    .addFamily(new HColumnDescriptor("cf_info")));
        }

        table = conn.getTable(tableName);

        scan = new Scan();
        scan.addFamily(Bytes.toBytes(cf1));

    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> iterator = rs.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            String rowkey = Bytes.toString(result.getRow());
            StringBuffer sb = new StringBuffer();
            for (Cell cell : result.listCells()) {
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                sb.append(value).append(",");
            }
            String valueString = sb.replace(sb.length() - 1, sb.length(), "").toString();
            Tuple2<String, String> tuple2 = new Tuple2<>();
            tuple2.setFields(rowkey, valueString);
            sourceContext.collect(tuple2);
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() {
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            logger.error("Close HBase Exception:", e.toString());
        }
    }
}
